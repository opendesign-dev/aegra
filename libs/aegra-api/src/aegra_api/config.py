"""Configuration management for Aegra HTTP settings"""

import json
from pathlib import Path
from typing import Literal, TypedDict

import structlog
from langgraph.store.base import TTLConfig
from langgraph.store.postgres.base import PostgresIndexConfig

from aegra_api.settings import settings

logger = structlog.get_logger(__name__)


class CorsConfig(TypedDict, total=False):
    """CORS configuration options"""

    allow_origins: list[str]
    allow_methods: list[str]
    allow_headers: list[str]
    allow_credentials: bool
    expose_headers: list[str]
    max_age: int


class HttpConfig(TypedDict, total=False):
    """HTTP configuration options for custom routes"""

    app: str
    """Import path for custom Starlette/FastAPI app to mount"""
    enable_custom_route_auth: bool
    """Apply Aegra authentication dependency to custom routes (uses FastAPI dependencies, not middleware)"""
    cors: CorsConfig | None
    """Custom CORS configuration"""
    disable_mcp: bool
    """Disable the /mcp endpoint that exposes assistants as MCP tools. Enabled by default."""
    disable_a2a: bool
    """Disable the /a2a/{assistant_id} endpoint and its agent card. Enabled by default."""


class StoreIndexConfig(PostgresIndexConfig, total=False):
    """Configuration for vector embeddings in store.

    Enables semantic similarity search using pgvector.
    See: https://github.com/aegra/aegra/issues/104

    Extends the store's own ``PostgresIndexConfig`` rather than restating it, so this
    dict is what `AsyncPostgresStore` accepts — including keys documented upstream but
    not listed here (``ann_index_config``, ``distance_type``).

    Keys used most often:

    - ``dims``: embedding vector dimensions (1536 for OpenAI text-embedding-3-small)
    - ``embed``: model as ``'<provider>:<model-id>'``, e.g. ``openai:text-embedding-3-small``,
      ``bedrock:amazon.titan-embed-text-v2:0``, ``cohere:embed-english-v3.0``
    - ``fields``: JSON fields to embed, defaulting to ``["$"]`` (whole document);
      also accepts top-level names or JSON paths like ``["metadata.title"]``
    """


class StoreConfig(TypedDict, total=False):
    """Store configuration options"""

    index: StoreIndexConfig | None
    """Vector index configuration for semantic search"""
    scopes: dict[str, list[str]]
    """Map of namespace prefix -> list of User attributes used for configurable store scoping."""

    ttl: TTLConfig | None
    """Retention policy for store items, applied by the store's own sweeper.

    Keys match LangGraph Platform's ``store.ttl`` because this is the upstream
    ``TTLConfig`` verbatim: ``refresh_on_read``, ``default_ttl`` (minutes),
    ``sweep_interval_minutes``. Omitted means items never expire.
    """


class CheckpointerTTLConfig(TypedDict, total=False):
    """Retention policy for threads and their checkpoints.

    Key names, defaults and semantics follow LangGraph Platform's ``checkpointer.ttl``.
    Unlike ``store.ttl`` this has no upstream implementation to delegate to — the
    open-source checkpointer ships no sweeper — so Aegra drives expiry itself.
    """

    strategy: Literal["delete", "keep_latest"]
    """``delete`` drops the thread with its runs and checkpoints; ``keep_latest`` keeps the
    thread and its current state, discarding only the history behind it."""

    default_ttl: float | None
    """Lifetime in minutes for threads that carry no ``ttl`` of their own. Unset means never."""

    sweep_interval_minutes: float
    """How often expired threads are looked for."""

    sweep_limit: int
    """Threads reclaimed per pass, bounding how long one pass holds row locks."""


class CheckpointerConfig(TypedDict, total=False):
    """Checkpointer configuration options."""

    ttl: CheckpointerTTLConfig | None
    """Retention policy; omitted leaves threads and checkpoints in place forever."""


class AuthConfig(TypedDict, total=False):
    """Auth configuration options."""

    path: str
    """Import path for auth handler in format './file.py:variable' or 'module:variable'.
    Examples:
    - './auth.py:auth' - Load 'auth' from auth.py in project root
    - './src/auth/firebase.py:auth' - Load from nested path
    - 'mypackage.auth:auth' - Load from installed package
    """
    disable_studio_auth: bool
    """Disable authentication for LangGraph Studio connections"""


def _resolve_config_path() -> Path | None:
    """Resolve config file path using standard resolution order.

    Resolution order:
    1) AEGRA_CONFIG env var (if set and file exists)
    2) aegra.json in CWD
    3) langgraph.json in CWD (fallback for compatibility)

    Returns:
        Path to config file or None if not found
    """
    # 1) Env var override - only use if file actually exists
    if env_path := settings.app.AEGRA_CONFIG:
        path = Path(env_path)
        if path.exists():
            return path
        logger.warning(f"AEGRA_CONFIG={env_path!r} not found, falling back to config discovery")

    # 2) aegra.json if present
    aegra_path = Path("aegra.json")
    if aegra_path.exists():
        return aegra_path

    # 3) fallback to langgraph.json
    langgraph_path = Path("langgraph.json")
    if langgraph_path.exists():
        return langgraph_path

    return None


def load_config() -> dict | None:
    """Load full config file using standard resolution order.

    Returns:
        Full config dict or None if not found
    """
    config_path = _resolve_config_path()
    if not config_path:
        return None

    try:
        with config_path.open() as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logger.warning(f"Config file {config_path} does not contain a JSON object")
            return None
        return data
    except Exception as e:
        logger.warning(f"Failed to load config from {config_path}: {e}")
        return None


def load_http_config() -> HttpConfig | None:
    """Load HTTP config from aegra.json or langgraph.json.

    Uses standard config resolution order.

    Returns:
        HTTP configuration dict or None if not found
    """
    config = load_config()
    if config is None:
        return None

    http_config = config.get("http")
    if http_config:
        config_path = _resolve_config_path()
        logger.info(f"Loaded HTTP config from {config_path}")
        return http_config

    return None


def load_checkpointer_ttl_config() -> CheckpointerTTLConfig | None:
    """Load ``checkpointer.ttl`` from aegra.json or langgraph.json.

    Absent config leaves retention off, which is the behaviour every existing deployment has.
    """
    config = load_config()
    if config is None:
        return None

    ttl_config = (config.get("checkpointer") or {}).get("ttl")
    if ttl_config:
        logger.info(f"Loaded checkpointer TTL config from {_resolve_config_path()}")
        return ttl_config

    return None


def load_store_config() -> StoreConfig | None:
    """Load store config from aegra.json or langgraph.json.

    Uses standard config resolution order.

    Returns:
        Store configuration dict or None if not found
    """
    config = load_config()
    if config is None:
        return None

    store_config = config.get("store")
    if store_config:
        config_path = _resolve_config_path()
        logger.info(f"Loaded store config from {config_path}")
        return store_config

    return None


def load_auth_config() -> AuthConfig | None:
    """Load auth config from aegra.json or langgraph.json.

    Uses standard config resolution order.

    Returns:
        Auth configuration dict or None if not found
    """
    config = load_config()
    if config is None:
        return None

    auth_config = config.get("auth")
    if auth_config:
        config_path = _resolve_config_path()
        logger.info(f"Loaded auth config from {config_path}")
        return auth_config

    return None


def get_config_dir() -> Path | None:
    """Get the directory containing the config file.

    This is used to resolve relative paths in the config file
    (graphs, http.app, auth.path) relative to the config location.

    Returns:
        Path to config directory or None if no config found
    """
    config_path = _resolve_config_path()
    if config_path and config_path.exists():
        return config_path.parent.resolve()
    return None
