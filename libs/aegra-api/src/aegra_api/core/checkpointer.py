"""Postgres checkpointer with per-run deletion.

Extends langgraph's ``AsyncPostgresSaver``, which through checkpoint-postgres
3.1.0 implements only whole-thread deletion (``adelete_thread``).
``adelete_for_runs`` is declared on the base saver but left
``NotImplementedError``; this fills it in for Aegra's rollback double-texting,
deleting exactly the checkpoints a run produced while keeping blobs that
surviving checkpoints still reference.
"""

import os
from collections.abc import Iterable, Sequence
from typing import Any

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.serde.encrypted import EncryptedSerializer
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


def build_encrypted_serde() -> SerializerProtocol | None:
    """Return an AES checkpoint serializer when ``LANGGRAPH_AES_KEY`` is set.

    Encryption at rest for checkpoint state/blobs via langgraph's native
    ``EncryptedSerializer``. None (plaintext) when unset — plaintext rows still
    decrypt after enabling it since the serializer keys off the cipher tag.
    """
    if not os.getenv("LANGGRAPH_AES_KEY"):
        return None
    return EncryptedSerializer.from_pycryptodome_aes()


# Checkpoints carry their creating run in ``metadata->>'run_id'`` — langgraph
# writes it from the top-level config ``run_id``, which Aegra pins to its own
# run id (see langgraph_service.create_run_config).
_AFFECTED_THREADS_SQL = "SELECT DISTINCT thread_id FROM checkpoints WHERE metadata->>'run_id' = ANY(%s)"

_DELETE_WRITES_SQL = (
    "DELETE FROM checkpoint_writes cw USING checkpoints c "
    "WHERE cw.thread_id = c.thread_id AND cw.checkpoint_ns = c.checkpoint_ns "
    "AND cw.checkpoint_id = c.checkpoint_id AND c.metadata->>'run_id' = ANY(%s)"
)

_DELETE_CHECKPOINTS_SQL = "DELETE FROM checkpoints WHERE metadata->>'run_id' = ANY(%s)"

# GC blobs no surviving checkpoint references, matching the saver's own
# checkpoint->blob join on (channel, version): correct across namespaces and
# delta channels without version arithmetic.
_GC_BLOBS_SQL = (
    "DELETE FROM checkpoint_blobs b WHERE b.thread_id = ANY(%s) AND NOT EXISTS ("
    " SELECT 1 FROM checkpoints c"
    " JOIN jsonb_each_text(c.checkpoint -> 'channel_versions') cv"
    " ON cv.key = b.channel AND cv.value = b.version"
    " WHERE c.thread_id = b.thread_id AND c.checkpoint_ns = b.checkpoint_ns"
    ")"
)

# One row per checkpoint: parent link, which channels store a real value here,
# and which are delta-backed.
#
# ``type <> 'empty'`` is langgraph's "populated" test — _dump_blobs writes
# ('empty', NULL) for a channel absent from channel_values, and
# DeltaChannel.checkpoint() returns MISSING on non-snapshot steps, so a delta
# channel is populated only at its snapshot steps.
#
# ``metadata->counters_since_delta_snapshot`` is keyed by delta channel name.
# Part of langgraph's beta DeltaChannel surface; if it ever disappears the keys
# come back empty and prune degrades to keeping just the latest checkpoint.
_CHECKPOINT_GRAPH_SQL = """
SELECT c.checkpoint_ns,
       c.checkpoint_id,
       c.parent_checkpoint_id,
       COALESCE(
         (SELECT array_agg(b.channel)
            FROM checkpoint_blobs b
           WHERE b.thread_id = c.thread_id
             AND b.checkpoint_ns = c.checkpoint_ns
             AND b.version = c.checkpoint -> 'channel_versions' ->> b.channel
             AND b.type <> 'empty'),
         ARRAY[]::text[]
       ) AS populated,
       COALESCE(
         (SELECT array_agg(key)
            FROM jsonb_object_keys(
                   COALESCE(c.metadata -> 'counters_since_delta_snapshot', '{}'::jsonb)
                 ) AS key),
         ARRAY[]::text[]
       ) AS delta_channels
  FROM checkpoints c
 WHERE c.thread_id = %s
 ORDER BY c.checkpoint_ns, c.checkpoint_id
"""

_PRUNE_WRITES_BY_ID_SQL = (
    "DELETE FROM checkpoint_writes WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = ANY(%s)"
)
_PRUNE_CHECKPOINTS_BY_ID_SQL = (
    "DELETE FROM checkpoints WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = ANY(%s)"
)


def _keep_set(rows: Iterable[dict[str, Any]]) -> set[str]:
    """Checkpoint ids that must survive a keep_latest prune, for one namespace.

    Keeps the newest checkpoint, then walks the parent chain keeping every
    ancestor until each delta channel the newest one does not store itself has
    been seeded. A delta channel stores a value only at its snapshot steps and
    rebuilds from ancestor writes, so dropping that stretch would leave it
    reconstructing as empty with no error raised.

    Scoped to delta channels on purpose: plain channels carry their value in the
    kept checkpoint, and transient ones (``__start__``, ``branch:to:*``) are empty
    in every checkpoint — treating those as unseeded would walk to the root and
    keep the entire chain, defeating the prune.
    """
    ordered = list(rows)
    if not ordered:
        return set()

    by_id = {str(r["checkpoint_id"]): r for r in ordered}
    latest = ordered[-1]  # checkpoint ids are time-ordered uuid6
    keep = {str(latest["checkpoint_id"])}

    populated = set(latest["populated"] or ())
    unseeded = {str(c) for c in (latest["delta_channels"] or ())} - populated

    cursor = latest.get("parent_checkpoint_id")
    while unseeded and cursor:
        parent = by_id.get(str(cursor))
        if parent is None:
            break
        keep.add(str(parent["checkpoint_id"]))
        unseeded -= set(parent["populated"] or ())
        cursor = parent.get("parent_checkpoint_id")
    return keep


def _doomed_by_namespace(rows: Iterable[dict[str, Any]]) -> dict[str, list[str]]:
    """Group checkpoint rows per namespace and return the ids safe to delete."""
    per_ns: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        per_ns.setdefault(str(row["checkpoint_ns"]), []).append(row)

    doomed: dict[str, list[str]] = {}
    for namespace, ns_rows in per_ns.items():
        keep = _keep_set(ns_rows)
        doomed[namespace] = [str(r["checkpoint_id"]) for r in ns_rows if str(r["checkpoint_id"]) not in keep]
    return doomed


class AegraPostgresSaver(AsyncPostgresSaver):
    """``AsyncPostgresSaver`` plus per-run checkpoint deletion."""

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        """Delete every checkpoint (and its writes) produced by the given runs.

        Matches checkpoints via ``metadata->>'run_id'``, drops their pending
        writes, then GCs blobs no surviving checkpoint still references. Runs
        as one transaction so the blob GC observes the checkpoint deletions.

        Delta channels: deleting a run whose checkpoints hold the only
        ``_DeltaSnapshot`` blob — or whose writes a still-live descendant
        depends on — can break ``DeltaChannel`` reconstruction, per the base
        saver's contract. Safe for rollback: the run's checkpoints are the
        thread's forward tail, so nothing surviving depends on them.
        """
        ids = [str(rid) for rid in run_ids]
        if not ids:
            return
        pool = self.conn
        if not isinstance(pool, AsyncConnectionPool):
            raise TypeError("AegraPostgresSaver requires a connection pool")
        async with pool.connection() as conn, conn.transaction():
            cur = await conn.execute(_AFFECTED_THREADS_SQL, (ids,))
            thread_ids = [row["thread_id"] for row in await cur.fetchall()]
            if not thread_ids:
                return
            await conn.execute(_DELETE_WRITES_SQL, (ids,))
            await conn.execute(_DELETE_CHECKPOINTS_SQL, (ids,))
            await conn.execute(_GC_BLOBS_SQL, (thread_ids,))

    async def aprune_keep_latest(self, thread_ids: Sequence[str]) -> None:
        """Keep the latest checkpoint per namespace, plus the ancestors it needs.

        Deletes superseded checkpoints and their writes, then GCs blobs no
        surviving checkpoint references, in one transaction.

        ``DeltaChannel``-aware per the base saver's ``prune`` contract: a delta
        channel stores a value only at its snapshot steps and reconstructs by
        replaying ancestor writes, so ancestors between the kept checkpoint and
        each channel's nearest seed are preserved. Dropping them would leave the
        channel reconstructing as empty with no error raised.
        """
        ids = [str(tid) for tid in thread_ids]
        if not ids:
            return
        pool = self.conn
        if not isinstance(pool, AsyncConnectionPool):
            raise TypeError("AegraPostgresSaver requires a connection pool")
        async with pool.connection() as conn, conn.transaction():
            for thread_id in ids:
                cur = await conn.cursor(row_factory=dict_row).execute(_CHECKPOINT_GRAPH_SQL, (thread_id,))
                rows = await cur.fetchall()
                for namespace, doomed in _doomed_by_namespace(rows).items():
                    if not doomed:
                        continue
                    await conn.execute(_PRUNE_WRITES_BY_ID_SQL, (thread_id, namespace, doomed))
                    await conn.execute(_PRUNE_CHECKPOINTS_BY_ID_SQL, (thread_id, namespace, doomed))
            await conn.execute(_GC_BLOBS_SQL, (ids,))
