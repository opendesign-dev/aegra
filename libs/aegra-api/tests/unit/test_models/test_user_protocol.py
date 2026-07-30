"""Unit tests for the User model's LangGraph BaseUser protocol conformance."""

import pytest
from langgraph_sdk.auth.types import BaseUser

from aegra_api.models.auth import User


class TestDisplayName:
    """display_name must always be a str — BaseUser declares it non-null."""

    def test_defaults_to_identity_when_omitted(self) -> None:
        assert User(identity="user-123").display_name == "user-123"

    def test_defaults_to_identity_when_explicitly_none(self) -> None:
        assert User(identity="user-123", display_name=None).display_name == "user-123"

    def test_preserves_explicit_value(self) -> None:
        assert User(identity="user-123", display_name="Test User").display_name == "Test User"


class TestMappingAccess:
    """@auth.on handlers may read the user as a mapping instead of an object."""

    def test_getitem_returns_declared_field(self) -> None:
        user = User(identity="user-123", permissions=["read"])

        assert user["identity"] == "user-123"
        assert user["permissions"] == ["read"]

    def test_getitem_returns_extra_field(self) -> None:
        user = User(identity="user-123", subscription_tier="pro")

        assert user["subscription_tier"] == "pro"

    def test_getitem_raises_keyerror_for_unknown_key(self) -> None:
        user = User(identity="user-123")

        with pytest.raises(KeyError):
            user["team_id"]

    def test_contains_finds_declared_field(self) -> None:
        assert "identity" in User(identity="user-123")

    def test_contains_finds_extra_field(self) -> None:
        assert "team_id" in User(identity="user-123", team_id="team-1")

    def test_contains_is_false_for_unknown_key(self) -> None:
        assert "team_id" not in User(identity="user-123")


def test_satisfies_base_user_protocol() -> None:
    assert isinstance(User(identity="user-123"), BaseUser)
