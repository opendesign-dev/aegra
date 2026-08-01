"""Tenant scoping for collection queries.

The ``user_id`` column predicate — not the ``@auth.on`` handler — is the tenant
boundary, because handlers are default-allow when none is registered
(GHSA-m98r-6667-4wq7). Widening it is therefore an explicit, per-resource opt-in
rather than something a handler can grant by returning a looser filter.

Read-only and collection-only by design: ``<resource>:read_all`` widens search,
count, and list. Fetching, mutating, or deleting another identity's row stays
impossible regardless of permissions.
"""

from typing import Literal

from sqlalchemy import or_, true
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement

from aegra_api.models.auth import User

ScopedResource = Literal["assistants", "threads", "runs", "crons"]

# Assistants seeded from aegra.json are owned by this identity and readable by all.
SYSTEM_IDENTITY = "system"


def read_all_permission(resource: ScopedResource) -> str:
    """Permission string that lets a caller read other identities' rows."""
    return f"{resource}:read_all"


def owned_or_system(owner: InstrumentedAttribute[str], user: User) -> ColumnElement[bool]:
    """The caller's own rows plus the seeded ones; the boundary for reading a single assistant."""
    return or_(owner == user.identity, owner == SYSTEM_IDENTITY)


def read_scope(
    owner: InstrumentedAttribute[str],
    user: User,
    *,
    resource: ScopedResource,
    include_system: bool = False,
) -> ColumnElement[bool]:
    """Predicate selecting the rows *user* may read.

    Always returns a predicate — ``true()`` when widened — so a call site keeps
    its ``where()`` and cannot silently lose the boundary during a refactor.
    """
    if read_all_permission(resource) in user.permissions:
        return true()
    if include_system:
        return owned_or_system(owner, user)
    return owner == user.identity
