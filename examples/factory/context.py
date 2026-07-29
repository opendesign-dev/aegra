"""Typed factory context — drives model selection, graph structure, and MCP lifecycle."""

import os
from dataclasses import dataclass, field


@dataclass(kw_only=True)
class FactoryContext:
    """Request context for the unified factory example.

    Callers pass this as ``context={...}`` when creating a run. Fields are
    used at two different levels:

    **Factory level** (``ServerRuntime[FactoryContext]``):
        ``enable_search`` and ``enable_mcp`` control graph structure and
        resource lifecycle — decisions that must happen before execution.

    **Node level** (``Runtime[FactoryContext]``):
        ``model`` and ``system_prompt`` are read inside node functions
        during execution via LangGraph's standard runtime injection.

    Attributes:
        model: LLM identifier in ``provider/model-name`` format.
        system_prompt: System message prepended to every LLM call.
        enable_search: When ``False``, the ``search_web`` tool is excluded
            and the graph has no ``tools`` node.
        enable_mcp: When ``True`` (and in an execution context), spins up
            MCP tool servers and adds their tools to the agent.
    """

    # MODEL matches react_agent's Context env convention, so a gateway that only
    # serves specific models can be pointed at without editing this file.
    model: str = field(default_factory=lambda: os.environ.get("MODEL", "openai/gpt-4o-mini"))
    system_prompt: str = "You are a helpful assistant."
    enable_search: bool = True
    enable_mcp: bool = False
