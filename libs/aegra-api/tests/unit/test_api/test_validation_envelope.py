"""Unit tests for the 422 error envelope the SDK can actually read.

``langgraph_sdk`` extracts an error message from ``message`` → ``detail`` →
``error``, taking the first that is a non-empty string. FastAPI's default 422
puts a list under ``detail``, so the client fell back to a bare
"422 Unprocessable Entity" and never learned which field was wrong.
"""

import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from starlette.testclient import TestClient

from aegra_api.main import exception_handlers


class _Body(BaseModel):
    name: str
    count: int = Field(..., ge=1)


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    for exc_type, handler in exception_handlers.items():
        app.add_exception_handler(exc_type, handler)

    @app.post("/echo")
    async def echo(body: _Body) -> dict[str, str]:
        return {"name": body.name}

    return TestClient(app, raise_server_exceptions=False)


def test_missing_field_reports_its_location_in_message(client: TestClient) -> None:
    resp = client.post("/echo", json={"count": 2})

    assert resp.status_code == 422
    body = resp.json()
    assert body["error"] == "validation_error"
    assert "body.name" in body["message"]


def test_constraint_violation_reports_the_reason(client: TestClient) -> None:
    resp = client.post("/echo", json={"name": "x", "count": 0})

    assert "count" in resp.json()["message"]
    assert "greater than or equal" in resp.json()["message"]


def test_message_is_a_non_empty_string_the_sdk_can_extract(client: TestClient) -> None:
    """The whole point: a list here means the client sees a generic status line."""
    message = client.post("/echo", json={}).json()["message"]

    assert isinstance(message, str)
    assert message


def test_per_field_records_stay_available_under_details(client: TestClient) -> None:
    errors = client.post("/echo", json={}).json()["details"]["errors"]

    assert {tuple(err["loc"]) for err in errors} == {("body", "name"), ("body", "count")}


def test_details_are_json_serializable(client: TestClient) -> None:
    """Pydantic puts exception objects in ``ctx``; unencoded they 500 the handler."""
    assert client.post("/echo", json={"name": "x", "count": "abc"}).status_code == 422


def test_handler_is_registered_for_request_validation_error() -> None:
    assert exception_handlers[RequestValidationError].__name__ == "validation_exception_handler"


class TestCustomAppMerge:
    """Regression: the 422 handler must survive a custom-app deployment.

    ``merge_exception_handlers`` used to skip any type already present in
    ``app.exception_handlers``, and FastAPI seeds every app with its own
    ``RequestValidationError`` handler — so Aegra's envelope was silently dropped
    whenever ``aegra.json`` configured ``http.app``.
    """

    def test_framework_default_is_replaced(self) -> None:
        from fastapi.exception_handlers import request_validation_exception_handler

        from aegra_api.core.route_merger import merge_exception_handlers

        user_app = FastAPI()
        assert user_app.exception_handlers[RequestValidationError] is request_validation_exception_handler

        merge_exception_handlers(user_app, exception_handlers)

        assert user_app.exception_handlers[RequestValidationError] is not request_validation_exception_handler

    def test_a_real_user_handler_still_wins(self) -> None:
        from aegra_api.core.route_merger import merge_exception_handlers

        async def mine(request: object, exc: Exception) -> None:  # pragma: no cover - identity only
            return None

        user_app = FastAPI()
        user_app.exception_handlers[RequestValidationError] = mine

        merge_exception_handlers(user_app, exception_handlers)

        assert user_app.exception_handlers[RequestValidationError] is mine

    def test_merged_app_returns_the_agent_protocol_envelope(self) -> None:
        from aegra_api.core.route_merger import merge_exception_handlers

        user_app = FastAPI()

        @user_app.post("/echo")
        async def echo(body: _Body) -> dict[str, str]:
            return {"name": body.name}

        merge_exception_handlers(user_app, exception_handlers)
        resp = TestClient(user_app, raise_server_exceptions=False).post("/echo", json={})

        assert resp.status_code == 422
        assert resp.json()["error"] == "validation_error"
