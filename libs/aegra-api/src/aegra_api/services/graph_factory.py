"""Graph factory classification, runtime construction, and dispatch helpers.

Supports four factory signatures:
- 0 params: ``def make_graph() -> Graph``
- 1 param (config): ``def make_graph(config: RunnableConfig) -> Graph``
- 1 param (runtime): ``def make_graph(runtime: ServerRuntime) -> Graph``
- 2 params (either order): ``def make_graph(config, runtime: ServerRuntime)``

Factory detection happens at graph load time via ``classify_factory()``.
Per-request invocation is handled by ``invoke_factory()`` with the appropriate
``ServerRuntime`` variant constructed by ``build_server_runtime()``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import typing
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, Literal, get_args, get_origin

import structlog
from langgraph.graph import StateGraph
from langgraph.pregel import Pregel
from langgraph.store.base import BaseStore
from langgraph_sdk.auth.types import BaseUser
from langgraph_sdk.runtime import (
    ServerRuntime,
    _ExecutionRuntime,
    _ReadRuntime,
)

from aegra_api.core.auth_ctx import get_auth_ctx
from aegra_api.models.auth import User

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

AccessContext = Literal[
    "threads.create_run",
    "threads.update",
    "threads.read",
    "assistants.read",
]
"""Why the graph factory is being called.

- ``threads.create_run``: Full graph execution (nodes + edges).
- ``threads.update``: ``aupdate_state`` — applies writes to state channels.
- ``threads.read``: ``aget_state`` / ``aget_state_history`` — formats state snapshots.
- ``assistants.read``: Schema extraction, graph visualization.
"""

_HookType = Callable[["_RunnableConfig", "ServerRuntime"], dict[str, Any]]

# Maps graph_id → a callable that produces kwargs for the factory function.
# Populated by ``classify_factory()`` at graph load time.
_FACTORY_KWARGS: dict[str, _HookType] = {}

# Maps graph_id → the ``T`` from ``ServerRuntime[T]`` (or ``None`` if
# the factory uses plain ``ServerRuntime`` without parameterization).
# Populated by ``classify_factory()`` alongside ``_FACTORY_KWARGS``.
_FACTORY_CONTEXT_TYPES: dict[str, type | None] = {}

# Concrete runtime classes used for ``issubclass`` checks during classification.
_RUNTIME_CLASSES: tuple[type, ...] = (_ExecutionRuntime, _ReadRuntime)

# Type alias for RunnableConfig — LangGraph uses ``dict[str, Any]``.
_RunnableConfig = dict[str, Any]

# Namespace for resolving string annotations in factory functions.
# Lets users import ``ServerRuntime`` inside ``TYPE_CHECKING`` blocks
# and still have the factory classifier resolve it correctly.
_RUNTIME_LOCALNS: dict[str, Any] = {
    "ServerRuntime": ServerRuntime,
    "RunnableConfig": _RunnableConfig,
    "Config": _RunnableConfig,
}


# ---------------------------------------------------------------------------
# Factory classification
# ---------------------------------------------------------------------------


def classify_factory(fn: Callable, graph_id: str) -> None:
    """Inspect *fn*'s signature and register a dispatch hook if it accepts arguments.

    Idempotent — calling twice with the same *graph_id* is a no-op.

    Also extracts the ``T`` from ``ServerRuntime[T]`` annotations and stores
    it in ``_FACTORY_CONTEXT_TYPES`` so that ``coerce_context()`` can coerce
    the raw request context dict to ``T`` at invocation time.
    """
    if graph_id in _FACTORY_KWARGS:
        return
    hook, context_type = _classify_factory(fn)
    if hook is not None:
        _FACTORY_KWARGS[graph_id] = hook
        _FACTORY_CONTEXT_TYPES[graph_id] = context_type


def _is_runtime_annotation(annotation: Any) -> bool:
    """Return ``True`` if *annotation* refers to ``ServerRuntime`` or a concrete subclass.

    Handles:
    - The ``ServerRuntime`` TypeAliasType directly.
    - Parameterized forms like ``ServerRuntime[MyContext]``.
    - Concrete runtime classes (``_ExecutionRuntime``, ``_ReadRuntime``)
      and their subclasses.
    - ``Annotated[ServerRuntime, ...]`` wrappers.
    """
    if annotation is inspect.Parameter.empty:
        return False
    # Identity check against the ServerRuntime TypeAliasType
    if annotation is ServerRuntime:
        return True
    # issubclass check against concrete runtime classes
    if isinstance(annotation, type):
        try:
            return issubclass(annotation, _RUNTIME_CLASSES)
        except TypeError:
            return False
    # Handle parameterized types (ServerRuntime[MyContext], Annotated[...], Union)
    origin = get_origin(annotation)
    if origin is not None:
        if origin is ServerRuntime:
            return True
        # For Union (e.g. None | ServerRuntime) or Annotated, check all operands
        args = get_args(annotation)
        if args:
            return any(_is_runtime_annotation(a) for a in args)
    return False


def _extract_context_type(annotation: Any) -> type | None:
    """Extract ``T`` from a ``ServerRuntime[T]`` annotation."""
    if annotation is inspect.Parameter.empty or annotation is ServerRuntime:
        return None

    origin = get_origin(annotation)
    if origin is ServerRuntime:
        args = get_args(annotation)
        if args:
            return args[0]
        return None

    # Handle Union types (e.g. None | ServerRuntime[T])
    if origin is not None:
        args = get_args(annotation)
        if args:
            for arg in args:
                result = _extract_context_type(arg)
                if result is not None:
                    return result

    return None


def _resolve_hints(fn: Callable) -> dict[str, Any]:
    """Resolve string annotations using the function's module globals + runtime types."""
    try:
        return typing.get_type_hints(fn, localns=_RUNTIME_LOCALNS)
    except (NameError, AttributeError) as exc:
        logger.debug("graph_factory_hint_resolution_failed", fn=fn, exc=str(exc))
        return {}


def _classify_factory(fn: Callable) -> tuple[_HookType | None, type | None]:
    """Classify a graph factory by its parameter signature.

    Returns a tuple of:
    - A callable that, given ``(config, server_runtime)``, produces the
      ``**kwargs`` dict to pass to the factory. ``None`` for 0-arg factories.
    - The context type ``T`` from ``ServerRuntime[T]`` (or ``None``).

    Raises:
        ValueError: If the factory has 3+ parameters or ambiguous runtime params.
    """
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    hints = _resolve_hints(fn)

    def _annotation(p: inspect.Parameter) -> Any:
        return hints.get(p.name, p.annotation)

    if len(params) == 0:
        # 0-arg factory — no hook needed
        return None, None
    elif len(params) == 1:
        ann = _annotation(params[0])
        if _is_runtime_annotation(ann):
            ctx_type = _extract_context_type(ann)
            return lambda config, runtime: {params[0].name: runtime}, ctx_type
        return lambda config, runtime: {params[0].name: config}, None
    elif len(params) == 2:
        # Detect which param is runtime by annotation; the other is config.
        rt_indices = [i for i, p in enumerate(params) if _is_runtime_annotation(_annotation(p))]
        if len(rt_indices) == 1:
            rt_idx = rt_indices[0]
            cfg_idx = 1 - rt_idx
        elif len(rt_indices) == 0:
            raise ValueError(
                f"Graph factory {fn} has 2 parameters but neither is annotated as "
                f"ServerRuntime. For a 2-param factory, one parameter must be typed as "
                f"ServerRuntime and the other as RunnableConfig."
            )
        else:
            raise ValueError(
                f"Graph factory {fn} has 2 parameters both annotated as ServerRuntime. "
                f"Expected one ServerRuntime and one RunnableConfig."
            )
        ctx_type = _extract_context_type(_annotation(params[rt_idx]))
        return lambda config, runtime: {
            params[rt_idx].name: runtime,
            params[cfg_idx].name: config,
        }, ctx_type
    else:
        raise ValueError(
            f"Graph factory {fn} must take 0, 1, or 2 arguments. "
            f"Got {len(params)} parameters: {[p.name for p in params]}"
        )


# ---------------------------------------------------------------------------
# Factory state queries
# ---------------------------------------------------------------------------


def is_factory(graph_id: str) -> bool:
    """Return ``True`` if *graph_id* was classified as a factory that accepts arguments."""
    return graph_id in _FACTORY_KWARGS


def is_for_execution(access_context: AccessContext) -> bool:
    """Return ``True`` if the access context represents full graph execution."""
    return access_context == "threads.create_run"


# ---------------------------------------------------------------------------
# Context coercion
# ---------------------------------------------------------------------------


def coerce_context(context: dict[str, Any] | None, graph_id: str) -> Any:
    """Coerce a raw context dict into the factory's declared ``ServerRuntime[T]`` type.

    Pydantic models go through ``model_validate``, dataclasses through ``T(**context)``.
    A validation failure logs a warning and returns the raw dict rather than failing the run.
    """
    if context is None:
        return None

    ctx_type = _FACTORY_CONTEXT_TYPES.get(graph_id)
    if ctx_type is None:
        return context

    try:
        if _is_pydantic_model(ctx_type):
            return ctx_type.model_validate(context)
        if dataclasses.is_dataclass(ctx_type):
            return ctx_type(**context)
    except Exception as exc:
        logger.warning(
            "context_coercion_failed",
            graph_id=graph_id,
            context_type=ctx_type.__name__,
            exc=str(exc),
            msg="Falling back to raw dict",
        )
    return context


def _is_pydantic_model(cls: type) -> bool:
    """Return ``True`` if *cls* is a Pydantic ``BaseModel`` subclass.

    Uses duck-typing (``model_validate``) to avoid importing pydantic
    directly, which is an optional dependency at the service layer.
    """
    return hasattr(cls, "model_validate") and callable(getattr(cls, "model_validate", None))


# ---------------------------------------------------------------------------
# Runtime construction
# ---------------------------------------------------------------------------


def build_server_runtime(
    *,
    access_context: AccessContext,
    store: BaseStore | None,
    user: User | BaseUser | None = None,
    context: Any = None,
) -> ServerRuntime:
    """Build the ``ServerRuntime`` variant that *access_context* calls for.

    ``threads.create_run`` gets an ``_ExecutionRuntime`` carrying the coerced request
    context; every other context gets a ``_ReadRuntime``, which has no context field.
    A ``None`` *user* falls back to the current request's auth context.
    """
    if user is None:
        auth_ctx = get_auth_ctx()
        user = auth_ctx.user if auth_ctx else None

    if is_for_execution(access_context):
        return _ExecutionRuntime(
            access_context=access_context,
            user=user,
            store=store,
            context=context,
        )
    return _ReadRuntime(
        access_context=access_context,
        user=user,
        store=store,
    )


# ---------------------------------------------------------------------------
# Factory invocation
# ---------------------------------------------------------------------------


def invoke_factory(
    fn: Callable,
    graph_id: str,
    config: _RunnableConfig,
    server_runtime: ServerRuntime,
) -> Any:
    """Call a graph factory with the correct arguments based on its classification."""
    hook = _FACTORY_KWARGS.get(graph_id)
    if not hook:
        return fn()
    kwargs = hook(config, server_runtime)
    return fn(**kwargs)


# ---------------------------------------------------------------------------
# Graph result resolution
# ---------------------------------------------------------------------------


@asynccontextmanager
async def generate_graph(value: Any, graph_id: str) -> AsyncIterator[Pregel | StateGraph]:
    """Yield a graph object regardless of the factory's return type.

    Handles:
    - ``Pregel`` / ``StateGraph`` — yield directly.
    - Async context manager — ``async with value as graph: yield graph``.
    - Sync context manager — ``with value as graph: yield graph``.
    - Coroutine — ``yield await value``.
    - Other — yield as-is (likely a ``StateGraph`` or ``Pregel``).
    """
    if isinstance(value, Pregel | StateGraph):
        yield value
    elif hasattr(value, "__aenter__") and hasattr(value, "__aexit__"):
        async with value as ctx_value:
            logger.debug("graph_factory_async_ctx_resolved", graph_id=graph_id)
            yield ctx_value
    elif hasattr(value, "__enter__") and hasattr(value, "__exit__"):
        with value as ctx_value:
            logger.debug("graph_factory_sync_ctx_resolved", graph_id=graph_id)
            yield ctx_value
    elif asyncio.iscoroutine(value):
        result = await value
        logger.debug("graph_factory_coroutine_resolved", graph_id=graph_id)
        yield result
    else:
        yield value


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------


def clear_factory_registry(graph_id: str | None = None) -> None:
    """Remove factory dispatch hooks and context type registrations."""
    if graph_id is not None:
        _FACTORY_KWARGS.pop(graph_id, None)
        _FACTORY_CONTEXT_TYPES.pop(graph_id, None)
    else:
        _FACTORY_KWARGS.clear()
        _FACTORY_CONTEXT_TYPES.clear()
