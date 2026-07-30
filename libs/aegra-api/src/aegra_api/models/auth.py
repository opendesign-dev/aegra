"""Authentication and user context models"""

from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator


class User(BaseModel):
    """User model that accepts any auth fields.

    This model uses ConfigDict(extra="allow") to accept any additional fields
    from auth handlers (e.g., subscription_tier, team_id) while maintaining
    type hints for common fields.

    Implements the LangGraph ``BaseUser`` protocol so it can be handed straight
    to ``@auth.on`` handlers, which may read it either as an object
    (``user.identity``) or as a mapping (``user["identity"]``).
    """

    model_config = ConfigDict(extra="allow")

    # Required
    identity: str

    # Optional with defaults
    is_authenticated: bool = True
    permissions: list[str] = []
    display_name: str

    # Common optional fields (for IDE hints)
    org_id: str | None = None
    email: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _default_display_name(cls, data: Any) -> Any:
        """BaseUser requires a non-null display_name; fall back to identity."""
        if isinstance(data, dict) and data.get("display_name") is None:
            return {**data, "display_name": data.get("identity", "")}
        return data

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict including all extra fields."""
        return self.model_dump()

    def __getattr__(self, name: str) -> Any:
        """Allow attribute access to extra fields."""
        try:
            extra = object.__getattribute__(self, "__pydantic_extra__") or {}
        except AttributeError:
            extra = {}
        if name in extra:
            return extra[name]
        raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

    def __getitem__(self, key: str) -> Any:
        """Mapping access over declared fields and extras (BaseUser protocol)."""
        data = self.to_dict()
        if key not in data:
            raise KeyError(key)
        return data[key]

    def __contains__(self, key: str) -> bool:
        """Without this, ``key in user`` falls through to pydantic's pair-yielding
        ``__iter__`` and is always False."""
        return key in self.to_dict()


class AuthContext(BaseModel):
    """Authentication context for request processing"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    user: User
    request_id: str | None = None


class TokenPayload(BaseModel):
    """JWT token payload structure"""

    sub: str  # subject (user ID)
    name: str | None = None
    scopes: list[str] = []
    org: str | None = None
    exp: int | None = None
    iat: int | None = None
