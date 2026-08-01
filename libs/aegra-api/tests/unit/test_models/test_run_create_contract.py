"""Unit tests for the fields the LangGraph SDK sends on run creation.

Every field here used to be accepted and silently dropped, which is worse than a
4xx: the client sees 200 and assumes the option took effect. These lock in that
each one is either normalized onto a canonical field or validated.
"""

import pytest
from pydantic import ValidationError

from aegra_api.models import RunCreate


def _create(**kwargs: object) -> RunCreate:
    return RunCreate(assistant_id="agent", input={"messages": []}, **kwargs)


class TestCheckpointFlattening:
    def test_checkpoint_id_is_folded_into_checkpoint(self) -> None:
        """The SDK sends both shapes; only the nested one reaches the graph."""
        assert _create(checkpoint_id="cp-1").checkpoint == {"checkpoint_id": "cp-1"}

    def test_checkpoint_id_does_not_clobber_a_supplied_namespace(self) -> None:
        run = _create(checkpoint={"checkpoint_ns": "inner"}, checkpoint_id="cp-1")
        assert run.checkpoint == {"checkpoint_ns": "inner", "checkpoint_id": "cp-1"}

    def test_checkpoint_alone_still_satisfies_the_input_requirement(self) -> None:
        run = RunCreate(assistant_id="agent", checkpoint_id="cp-1")
        assert run.input is None and run.checkpoint == {"checkpoint_id": "cp-1"}


class TestDurability:
    def test_checkpoint_during_true_maps_to_async(self) -> None:
        assert _create(checkpoint_during=True).durability == "async"

    def test_checkpoint_during_false_maps_to_exit(self) -> None:
        assert _create(checkpoint_during=False).durability == "exit"

    def test_explicit_durability_wins_over_the_legacy_alias(self) -> None:
        assert _create(durability="sync", checkpoint_during=False).durability == "sync"

    def test_invalid_durability_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _create(durability="eventually")


class TestWebhook:
    def test_https_url_is_accepted(self) -> None:
        assert _create(webhook="https://example.test/hook").webhook == "https://example.test/hook"

    @pytest.mark.parametrize("url", ["ftp://example.test/x", "not-a-url", "https://"])
    def test_non_http_or_hostless_urls_are_rejected(self, url: str) -> None:
        """Delivery POSTs to whatever is stored, so this is the SSRF boundary."""
        with pytest.raises(ValidationError):
            _create(webhook=url)


class TestEnumsFollowTheSdk:
    @pytest.mark.parametrize("strategy", ["reject", "interrupt", "rollback", "enqueue"])
    def test_every_multitask_strategy_is_accepted(self, strategy: str) -> None:
        assert _create(multitask_strategy=strategy).multitask_strategy == strategy

    def test_unknown_multitask_strategy_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _create(multitask_strategy="queue")

    @pytest.mark.parametrize("mode", ["values", "messages-tuple", "checkpoints", "tasks", "events"])
    def test_stream_modes_match_the_sdk_literal(self, mode: str) -> None:
        assert _create(stream_mode=mode).stream_mode == mode

    def test_unknown_stream_mode_is_rejected(self) -> None:
        """Regression: an unvalidated stream_mode silently produced no events."""
        with pytest.raises(ValidationError):
            _create(stream_mode="verbose")

    @pytest.mark.parametrize("value", ["create", "reject"])
    def test_if_not_exists_accepts_both_behaviours(self, value: str) -> None:
        assert _create(if_not_exists=value).if_not_exists == value


class TestScheduling:
    def test_after_seconds_is_kept(self) -> None:
        assert _create(after_seconds=30).after_seconds == 30

    def test_negative_delay_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _create(after_seconds=-1)


class TestObservabilityPassthrough:
    def test_feedback_keys_and_tracer_are_retained(self) -> None:
        """Recorded rather than executed, but never dropped on the floor."""
        run = _create(feedback_keys=["helpfulness"], langsmith_tracer={"project_name": "p"})
        assert run.feedback_keys == ["helpfulness"]
        assert run.langsmith_tracer == {"project_name": "p"}
