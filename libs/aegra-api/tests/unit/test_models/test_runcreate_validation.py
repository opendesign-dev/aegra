"""Tests for RunCreate model validation."""

import pytest
from pydantic import ValidationError

from aegra_api.models.runs import RunCreate


class TestRunCreateValidation:
    """Tests for RunCreate input/command validation."""

    def test_checkpoint_only_payload_preserves_none_input(self):
        """Checkpoint-only payloads must keep input as None so LangGraph resumes
        from the checkpoint instead of restarting the graph from __start__.

        Regression test: previously the validator coerced input to ``{}`` which
        LangGraph Pregel treats as "new input" and re-enters __start__, ignoring
        the checkpoint's ``next=[...]``.
        """
        run_create = RunCreate(
            assistant_id="agent",
            checkpoint={"checkpoint_id": "chk-1", "checkpoint_ns": ""},
        )

        assert run_create.input is None
        assert run_create.command is None
        assert run_create.checkpoint == {"checkpoint_id": "chk-1", "checkpoint_ns": ""}

    def test_rejects_payload_without_input_command_or_checkpoint(self):
        """Ensure payloads with no input, command, or checkpoint are rejected."""
        with pytest.raises(ValueError, match="Must specify at least one of 'input', 'command', or 'checkpoint'"):
            RunCreate(assistant_id="agent")


class TestRunCreateMetadataValidation:
    """Tests for ``RunCreate.metadata`` shape enforcement.

    The schema constrains user-supplied metadata to OTEL-attribute
    primitives at request time so the contract is honest in the OpenAPI
    schema.  Previously ``dict[str, Any]`` accepted nested values that
    were silently dropped downstream by ``merge_run_metadata``; surfacing
    the rejection as a 422 means clients can fix the payload upstream
    rather than wonder why their metadata never reaches Langfuse.
    """

    def _payload(self, **overrides):
        base = {"assistant_id": "agent", "input": {"x": 1}}
        base.update(overrides)
        return base

    def test_none_metadata_accepted(self):
        run_create = RunCreate(**self._payload(metadata=None))
        assert run_create.metadata is None

    def test_empty_dict_metadata_accepted(self):
        run_create = RunCreate(**self._payload(metadata={}))
        assert run_create.metadata == {}

    def test_all_primitive_types_accepted(self):
        run_create = RunCreate(**self._payload(metadata={"tenant": "acme", "retries": 3, "ratio": 0.5, "flag": True}))
        assert run_create.metadata == {
            "tenant": "acme",
            "retries": 3,
            "ratio": 0.5,
            "flag": True,
        }

    def test_nested_dict_value_rejected(self):
        with pytest.raises(ValidationError):
            RunCreate(**self._payload(metadata={"k": {"nested": 1}}))

    def test_list_value_rejected(self):
        with pytest.raises(ValidationError):
            RunCreate(**self._payload(metadata={"k": [1, 2, 3]}))

    def test_too_many_keys_rejected(self):
        with pytest.raises(ValidationError, match="exceeds 32 keys"):
            RunCreate(**self._payload(metadata={f"k{i}": i for i in range(33)}))

    def test_dotted_key_rejected(self):
        """Reject keys containing ``.`` so users cannot land bare attributes
        like ``langfuse.user.id`` next to the system ones via the
        ``langfuse.trace.metadata.<key>`` channel."""
        with pytest.raises(ValidationError, match="must match"):
            RunCreate(**self._payload(metadata={"langfuse.user.id": "spoof"}))

    def test_empty_key_rejected(self):
        with pytest.raises(ValidationError, match="must match"):
            RunCreate(**self._payload(metadata={"": "v"}))

    def test_too_long_key_rejected(self):
        with pytest.raises(ValidationError, match="must match"):
            RunCreate(**self._payload(metadata={"k" * 65: "v"}))

    def test_non_ascii_key_rejected(self):
        """Reject non-ASCII keys to prevent visual-spoof of join keys
        (e.g. ``run_id`` vs ``run_id​`` zero-width-space variant)."""
        with pytest.raises(ValidationError, match="must match"):
            RunCreate(**self._payload(metadata={"run_id​": "v"}))

    def test_too_long_string_value_rejected(self):
        with pytest.raises(ValidationError, match="exceeds 512 characters"):
            RunCreate(**self._payload(metadata={"k": "v" * 513}))

    def test_max_keys_exactly_32_accepted(self):
        """Boundary: exactly 32 keys is allowed."""
        run_create = RunCreate(**self._payload(metadata={f"k{i}": i for i in range(32)}))
        assert len(run_create.metadata) == 32

    def test_max_value_length_exactly_512_accepted(self):
        """Boundary: exactly 512 chars is allowed."""
        run_create = RunCreate(**self._payload(metadata={"k": "v" * 512}))
        assert len(run_create.metadata["k"]) == 512


class TestRunCreateClientId:
    """``run_id`` is optional; the server generates one when it is omitted."""

    def test_defaults_to_none(self):
        assert RunCreate(assistant_id="agent", input={"x": 1}).run_id is None

    def test_accepts_client_value(self):
        assert RunCreate(assistant_id="agent", input={"x": 1}, run_id="my-run").run_id == "my-run"


class TestRunCommandValidation:
    """``command`` is a typed model so a misspelled key is a 422, not a silent no-op."""

    def _command(self, **command):
        return RunCreate(assistant_id="agent", command=command).command

    def test_resume_accepted(self):
        assert self._command(resume={"decision": "approve"}).resume == {"decision": "approve"}

    def test_resume_none_is_a_real_resume(self):
        """``None`` is a legitimate resume payload, so the key's presence is the signal."""
        command = self._command(resume=None)
        assert command.resume is None
        assert "resume" in command.model_fields_set

    def test_goto_and_update_accepted(self):
        command = self._command(goto="review", update={"approved": True})
        assert command.goto == "review"
        assert command.update == {"approved": True}

    def test_update_accepts_ordered_pairs(self):
        assert self._command(update=[("messages", "hi")]).update == [("messages", "hi")]

    def test_unknown_key_rejected(self):
        with pytest.raises(ValidationError, match="resumee"):
            self._command(resumee="approve")

    def test_empty_command_rejected(self):
        with pytest.raises(ValidationError, match="at least one of 'goto', 'update', or 'resume'"):
            self._command()

    def test_command_and_input_are_mutually_exclusive(self):
        with pytest.raises(ValidationError, match="mutually exclusive"):
            RunCreate(assistant_id="agent", input={"x": 1}, command={"resume": "yes"})


class TestInterruptSelectors:
    """Breakpoints take a node list or the ``*`` wildcard, matching the SDK type."""

    def _run(self, **fields):
        return RunCreate(assistant_id="agent", input={"x": 1}, **fields)

    def test_node_list_accepted(self):
        assert self._run(interrupt_before=["tools"]).interrupt_before == ["tools"]

    def test_wildcard_accepted(self):
        assert self._run(interrupt_after="*").interrupt_after == "*"

    def test_bare_node_name_rejected(self):
        """A single node must be given as a list; only '*' is a valid bare string."""
        with pytest.raises(ValidationError):
            self._run(interrupt_before="tools")
