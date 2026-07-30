"""Lightweight context-var helpers for passing authenticated user info into graphs.

Graph nodes can access the current request's authentication context by calling
`get_auth_ctx()`.  The server sets the context for the lifetime of a single run
(using an async context-manager) so the information is automatically scoped and
cleaned up.

The structure follows the standard auth context format so that
libraries expecting `Auth.types.BaseAuthContext` work unchanged.
"""

from __future__ import annotations

import contextvars
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from langgraph_sdk import Auth
from starlette.authentication import AuthCredentials, BaseUser

# Internal context-var storing the current auth context (or None when absent)
_AuthCtx: contextvars.ContextVar[Auth.types.BaseAuthContext | None] = contextvars.ContextVar(
    "AuthContext", default=None
)


def get_auth_ctx() -> Auth.types.BaseAuthContext | None:
    """Return the current authentication context or ``None`` if not set."""
    return _AuthCtx.get()


@asynccontextmanager
async def with_auth_ctx(
    user: BaseUser | None,
    permissions: list[str] | AuthCredentials | None = None,
) -> AsyncIterator[None]:
    """Temporarily set the auth context for the duration of an async block.

    Parameters
    ----------
    user
        The authenticated user (or ``None`` for anonymous access).
    permissions
        Either a Starlette ``AuthCredentials`` instance or a list of permission
        strings.  ``None`` means no permissions.
    """
    # Normalize the permissions list
    scopes: list[str] = []
    if isinstance(permissions, AuthCredentials):
        scopes = list(permissions.scopes)
    elif isinstance(permissions, list):
        scopes = permissions

    if user is None and not scopes:
        token = _AuthCtx.set(None)
    else:
        token = _AuthCtx.set(
            # Starlette's BaseUser and the SDK's are separate protocols with no
            # common base, and this branch is also reachable with user=None
            # (permissions but no identity). Both are pre-existing; narrowing them
            # would change auth semantics, so it is left for a dedicated change.
            Auth.types.BaseAuthContext(user=user, permissions=scopes)  # type: ignore[invalid-argument-type]
        )
    try:
        yield
    finally:
        _AuthCtx.reset(token)
