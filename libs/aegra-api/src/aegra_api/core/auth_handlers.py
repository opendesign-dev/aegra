"""Authorization handler support for @auth.on.* decorators.

This module provides integration with authorization handlers,
allowing users to define fine-grained access control rules using @auth.on.*
decorators in their auth.py files.
"""

from typing import Any

from fastapi import HTTPException
from langgraph_sdk import Auth
from langgraph_sdk.auth.types import AuthContext as LangGraphAuthContext

from aegra_api.core.auth_middleware import get_auth_instance
from aegra_api.models.auth import User


class AuthContextWrapper:
    """Wrapper to convert Aegra User model to AuthContext.

    AuthContext expects a BaseUser-compatible object. Our User model
    implements the BaseUser protocol (identity, is_authenticated, permissions,
    display_name, __getitem__, __contains__), so we can use it directly.
    """

    def __init__(
        self,
        user: User,
        resource: str,
        action: str,
    ) -> None:
        """Initialize auth context wrapper.

        Args:
            user: Authenticated user from Aegra's User model
            resource: Resource being accessed (e.g., "threads", "assistants")
            action: Action being performed (e.g., "create", "read", "update")
        """
        self.user = user
        self.resource = resource
        self.action = action
        self.permissions = user.permissions or []

    def to_langgraph_context(self) -> LangGraphAuthContext:
        """Convert to LangGraph AuthContext."""
        return LangGraphAuthContext(
            # BaseUser.__iter__ is declared unannotated in the SDK, so Pyright
            # infers `-> None` and rejects any real iterator — the SDK's own
            # StudioUser fails the same check. Every other member matches.
            user=self.user,  # pyright: ignore[reportArgumentType]
            resource=self.resource,  # type: ignore
            action=self.action,  # type: ignore
            permissions=self.permissions,
        )


async def handle_event(
    ctx: AuthContextWrapper | None,
    value: dict[str, Any],
) -> dict[str, Any] | None:
    """Call the most specific ``@auth.on.*`` handler and interpret its result.

    **Default-allow**: no auth configured, no matching handler, or a None/True return
    all allow the request. Only ``False`` or a raised exception denies — so raw Aegra
    works out of the box, and a tenant-scoped deployment must register a handler
    (see GHSA-m98r-6667-4wq7). A dict return is a query filter; ``value`` may be
    mutated in place to inject metadata.

    Resolution order: (resource, action) → (resource, "*") → ("*", action) → ("*", "*").

    Raises HTTPException 403 on deny (False / AssertionError), 500 on an unexpected
    exception or an invalid return type.
    """
    if ctx is None:
        # No auth context means no authorization check needed
        # This allows the request to proceed normally
        return None

    auth = get_auth_instance()
    if auth is None:
        # No auth configured, allow by default
        # This ensures raw Aegra works out-of-the-box without interruption
        return None

    # Convert to AuthContext
    auth_ctx = ctx.to_langgraph_context()

    # Find the most specific handler
    handler = _get_handler(auth, auth_ctx.resource, auth_ctx.action)
    if handler is None:
        # No handler for this resource/action, allow by default
        # Developers can use Aegra without defining handlers - it won't break
        return None

    try:
        # Call the handler with context and value
        result = await handler(ctx=auth_ctx, value=value)
    except Auth.exceptions.HTTPException as e:
        # Handler raised HTTP exception, convert to FastAPI HTTPException
        raise HTTPException(
            status_code=e.status_code,
            detail=e.detail,
            headers=dict(e.headers) if hasattr(e, "headers") and e.headers else None,
        ) from e
    except AssertionError as e:
        # Handler used assert for authorization check
        raise HTTPException(status_code=403, detail=str(e)) from e
    # Programmer errors (TypeError, AttributeError, ...) propagate so the
    # standard error handler logs the stack and returns a generic 500 — we
    # don't want a handler bug to leak its exception text to API clients.

    # Interpret handler result
    if result in (None, True):
        # Allow request, no filters
        return None

    if result is False:
        # Deny request
        raise HTTPException(status_code=403, detail="Forbidden")

    if isinstance(result, dict):
        # Return filter dict to apply to queries
        return result

    # Invalid return type
    raise HTTPException(
        status_code=500,
        detail=f"Auth handler returned invalid type: {type(result)}. Expected dict, None, True, or False.",
    )


def _get_handler(
    auth: Auth,
    resource: str,
    action: str,
) -> Any | None:
    """Find the most specific handler for resource+action.

    Handler resolution follows this priority order (most specific first):
    1. (resource, action) - e.g., ("threads", "create")
    2. (resource, "*") - e.g., ("threads", "*")
    3. ("*", action) - e.g., ("*", "create")
    4. ("*", "*") - global handler

    Args:
        auth: Auth instance with registered handlers
        resource: Resource name (e.g., "threads", "assistants")
        action: Action name (e.g., "create", "read", "update")

    Returns:
        Handler function or None if no handler found
    """
    # Check cache first
    key = (resource, action)
    if key in auth._handler_cache:
        return auth._handler_cache[key]

    # Priority order (most specific first)
    keys = [
        (resource, action),  # Most specific: exact resource+action
        (resource, "*"),  # Resource-specific: all actions on resource
        ("*", action),  # Action-specific: all resources for action
        ("*", "*"),  # Global: all resources and actions
    ]

    # Find first matching handler
    for check_key in keys:
        if check_key in auth._handlers and auth._handlers[check_key]:
            # Get the last registered handler (most recent wins)
            handler = auth._handlers[check_key][-1]
            # Cache the result
            auth._handler_cache[key] = handler
            return handler

    # Check global handlers (fallback for backward compatibility)
    if auth._global_handlers:
        handler = auth._global_handlers[-1]
        auth._handler_cache[key] = handler
        return handler

    return None


def build_auth_context(
    user: User,
    resource: str,
    action: str,
) -> AuthContextWrapper:
    """Wrap *user* with the resource/action pair that ``handle_event`` dispatches on."""
    return AuthContextWrapper(
        user=user,
        resource=resource,
        action=action,
    )


def merge_auth_filters(
    config: dict[str, Any],
    context: dict[str, Any],
    filters: dict[str, Any] | None,
    value: dict[str, Any],
) -> None:
    """Merge an ``@auth.on`` handler's config/context into an inline execution config.

    For the A2A/MCP paths that build an execution ``config`` dict (rather than a
    ``RunCreate``): use the returned filter dict when the handler produced one, else
    the ``value`` it mutated in place. The server ``thread_id`` stays authoritative if
    a handler replaced ``configurable``.
    """
    thread_id = config.get("configurable", {}).get("thread_id")
    source = filters if filters else value
    handler_config = source.get("config")
    if isinstance(handler_config, dict):
        config.update(handler_config)
    handler_context = source.get("context")
    if isinstance(handler_context, dict):
        context.update(handler_context)
    configurable = config.setdefault("configurable", {})
    if isinstance(configurable, dict) and thread_id is not None:
        configurable["thread_id"] = thread_id
