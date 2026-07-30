"""Test client fixtures"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aegra_api.core.auth_deps import get_current_user, require_auth
from aegra_api.core.health import router as health_router
from aegra_api.models.auth import User


def create_test_app(
    include_runs: bool = True,
    include_threads: bool = True,
    *,
    permissions: list[str] | None = None,
) -> FastAPI:
    """Build a FastAPI app with routers mounted and configured auth mocks.

    This setup automatically handles authentication overrides, ensuring that
    tests run as 'test-user' without encountering 401 errors. Pass ``permissions``
    to exercise the ``<resource>:search:all`` scoping gates.
    """
    app = FastAPI()

    # Dependency overrides rather than middleware: the middleware path built an
    # "anonymous" user, which defeats per-user scoping assertions.
    mock_user = User(identity="test-user", display_name="Test User", permissions=permissions or [])

    # 2. Override dependencies
    # require_auth: allows access to protected routes
    app.dependency_overrides[require_auth] = lambda: mock_user

    # get_current_user: ensures the 'user' variable inside the route equals our mock_user
    app.dependency_overrides[get_current_user] = lambda: mock_user

    app.include_router(health_router)

    if include_threads:
        from aegra_api.api import thread_state as thread_state_module
        from aegra_api.api import threads as threads_module

        # Same order as main._include_core_routers: state routes before the record routes.
        app.include_router(thread_state_module.router)
        app.include_router(threads_module.router)

    if include_runs:
        from aegra_api.api import runs as runs_module
        from aegra_api.api import stateless_runs as stateless_runs_module

        app.include_router(runs_module.router)
        app.include_router(stateless_runs_module.router)

    return app


def make_client(app: FastAPI) -> TestClient:
    """Create a test client for the given app"""
    return TestClient(app)
