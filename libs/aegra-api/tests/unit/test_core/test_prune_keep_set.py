"""Unit tests for the keep-set behind DeltaChannel-aware prune.

Regression: prune kept only the newest checkpoint per namespace, which severs a
delta channel's reconstruction chain — the channel then rebuilds as empty with no
error raised. Verified against a real database before these were written: a
5-step delta thread went from 10 items to 0.

Row shape mirrors _CHECKPOINT_GRAPH_SQL: populated = channels storing a real
value here, delta_channels = keys of metadata->counters_since_delta_snapshot.
"""

from typing import Any

from aegra_api.core.checkpointer import _doomed_by_namespace, _keep_set


def _row(
    checkpoint_id: str,
    parent: str | None = None,
    *,
    populated: list[str] | None = None,
    delta: list[str] | None = None,
    namespace: str = "",
) -> dict[str, Any]:
    return {
        "checkpoint_ns": namespace,
        "checkpoint_id": checkpoint_id,
        "parent_checkpoint_id": parent,
        "populated": populated or [],
        "delta_channels": delta or [],
    }


class TestPlainChannels:
    """Without delta channels the newest checkpoint is enough."""

    def test_empty_input(self) -> None:
        assert _keep_set([]) == set()

    def test_single_checkpoint_kept(self) -> None:
        assert _keep_set([_row("c1", populated=["messages"])]) == {"c1"}

    def test_only_newest_kept(self) -> None:
        """A plain channel stores its value in the kept checkpoint."""
        rows = [
            _row("c1", populated=["messages"]),
            _row("c2", "c1", populated=["messages"]),
            _row("c3", "c2", populated=["messages"]),
        ]
        assert _keep_set(rows) == {"c3"}

    def test_transient_channels_do_not_force_retention(self) -> None:
        """__start__ is empty in every checkpoint; it must not pin the chain.

        Regression: keying off channel_versions instead of delta_channels made
        every walk run to the root, so nothing was ever pruned.
        """
        rows = [
            _row("c1", populated=["messages"]),
            _row("c2", "c1", populated=["messages"]),
        ]
        assert _keep_set(rows) == {"c2"}


class TestDeltaChannels:
    """A delta channel pins ancestors back to its nearest seed."""

    def test_latest_is_its_own_snapshot(self) -> None:
        """When the newest checkpoint stores the value, no ancestor is needed."""
        rows = [
            _row("c1", delta=["items"]),
            _row("c2", "c1", populated=["items"], delta=["items"]),
        ]
        assert _keep_set(rows) == {"c2"}

    def test_walks_back_to_the_snapshot_ancestor(self) -> None:
        """Ancestors between the newest checkpoint and the seed are preserved."""
        rows = [
            _row("c1", delta=["items"]),
            _row("c2", "c1", populated=["items"], delta=["items"]),  # snapshot
            _row("c3", "c2", delta=["items"]),
            _row("c4", "c3", delta=["items"]),
        ]
        # c4 (newest) needs a seed; c3 has none; c2 supplies it and ends the walk.
        assert _keep_set(rows) == {"c4", "c3", "c2"}

    def test_no_snapshot_keeps_the_whole_chain(self) -> None:
        """A thread shorter than snapshot_frequency has no seed anywhere.

        Reconstruction is then "start empty + replay every write", so every
        ancestor's writes are load-bearing. This is the case that silently lost
        data before the fix.
        """
        rows = [
            _row("c1", delta=["items"]),
            _row("c2", "c1", delta=["items"]),
            _row("c3", "c2", delta=["items"]),
        ]
        assert _keep_set(rows) == {"c1", "c2", "c3"}

    def test_multiple_delta_channels_take_the_deeper_walk(self) -> None:
        """The walk ends only once every delta channel has been seeded."""
        rows = [
            _row("c1", populated=["a"], delta=["a", "b"]),  # seeds a
            _row("c2", "c1", populated=["b"], delta=["a", "b"]),  # seeds b
            _row("c3", "c2", delta=["a", "b"]),
        ]
        # c3 needs both; c2 seeds b; c1 seeds a — so all three stay.
        assert _keep_set(rows) == {"c3", "c2", "c1"}

    def test_broken_parent_link_stops_the_walk(self) -> None:
        """A dangling parent id must not loop or raise."""
        rows = [_row("c2", "missing-parent", delta=["items"])]
        assert _keep_set(rows) == {"c2"}

    def test_missing_delta_metadata_degrades_to_latest_only(self) -> None:
        """If the beta metadata field vanishes, prune stays safe-but-aggressive."""
        rows = [
            _row("c1", populated=["items"]),
            _row("c2", "c1"),
        ]
        assert _keep_set(rows) == {"c2"}


class TestNamespaceGrouping:
    """Namespaces prune independently — subgraphs have their own chains."""

    def test_each_namespace_keeps_its_own_latest(self) -> None:
        rows = [
            _row("a1", populated=["messages"], namespace=""),
            _row("a2", "a1", populated=["messages"], namespace=""),
            _row("b1", populated=["messages"], namespace="sub"),
            _row("b2", "b1", populated=["messages"], namespace="sub"),
        ]
        doomed = _doomed_by_namespace(rows)
        assert doomed[""] == ["a1"]
        assert doomed["sub"] == ["b1"]

    def test_delta_namespace_retention_is_independent(self) -> None:
        """A delta chain in one namespace does not pin another namespace."""
        rows = [
            _row("a1", delta=["items"], namespace=""),
            _row("a2", "a1", delta=["items"], namespace=""),
            _row("b1", populated=["messages"], namespace="sub"),
            _row("b2", "b1", populated=["messages"], namespace="sub"),
        ]
        doomed = _doomed_by_namespace(rows)
        assert doomed[""] == []  # no seed anywhere, keep both
        assert doomed["sub"] == ["b1"]

    def test_nothing_to_delete_yields_empty_lists(self) -> None:
        rows = [_row("a1", populated=["messages"])]
        assert _doomed_by_namespace(rows) == {"": []}
