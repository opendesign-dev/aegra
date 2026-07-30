"""Regression: AssistantCreate / AssistantUpdate must not share mutable default dicts.

Pydantic v2 currently deep-copies on assignment so the historical
`Field({})` shape hasn't bitten us, but the pattern is brittle. These tests
pin the safe `default_factory=dict` behavior so a future revert can't sneak
shared state across instances.
"""

from aegra_api.models.assistants import AssistantCreate, AssistantUpdate


def test_assistant_create_defaults_do_not_share_state() -> None:
    a = AssistantCreate(graph_id="agent")
    b = AssistantCreate(graph_id="agent")
    assert a.config is not None
    assert a.context is not None
    assert a.metadata is not None

    a.config["x"] = 1
    a.context["y"] = 2
    a.metadata["z"] = 3

    assert b.config == {}
    assert b.context == {}
    assert b.metadata == {}


def test_assistant_update_omitted_fields_are_none_not_empty() -> None:
    """PATCH must distinguish "omitted" from "set to empty".

    Regression: these defaulted to ``{}``, so the service could not tell the two
    apart and every partial update wiped the assistant's stored config — which
    also collided with the (user_id, graph_id, config) uniqueness constraint.
    """
    request = AssistantUpdate()
    assert request.config is None
    assert request.context is None
    assert request.metadata is None


def test_assistant_update_accepts_explicit_empty_dict() -> None:
    """An explicit ``{}`` still reaches the service as a clear-it instruction."""
    request = AssistantUpdate(config={}, context={}, metadata={})
    assert request.config == {}
    assert request.context == {}
    assert request.metadata == {}


def test_assistant_create_defaults_are_distinct_instances() -> None:
    a = AssistantCreate(graph_id="agent")
    b = AssistantCreate(graph_id="agent")

    assert a.config is not b.config
    assert a.context is not b.context
    assert a.metadata is not b.metadata


def test_assistant_update_graph_id_defaults_to_none() -> None:
    """PATCH is a partial update: omitting graph_id must not rebind the graph.

    The field used to default to "agent", so any update that left it out
    silently repointed the assistant at the "agent" graph.
    """
    assert AssistantUpdate().graph_id is None
