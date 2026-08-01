"""Shape rules for metadata that reaches OTEL span attributes.

Run metadata surfaces as ``langfuse.trace.metadata.<key>`` (and the OpenInference
``metadata.<key>`` alias on Phoenix). Without bounds a tenant could submit
thousands of keys, megabyte values, or nested structures — silently dropped at
emit, or large enough to blow past collector limits.

Shared by runs and crons because cron metadata is forwarded onto every run it
fires: validating it only on the run would turn a long-accepted cron into a
failure at firing time, hours after the mistake was made.
"""

import re
from typing import Any

KEY_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
MAX_KEYS = 32
MAX_VALUE_LEN = 512

# Stamped onto every run a cron fires so the run can be filtered back to its
# schedule. Server-owned: callers may not set it, and its slot is reserved.
CRON_ID_KEY = "cron_id"


def validate_metadata(metadata: dict[str, Any] | None, *, max_keys: int = MAX_KEYS) -> dict[str, Any] | None:
    """Enforce key shape, key count, value type, and string-value length.

    Every check raises its own message rather than relying on a primitive union
    on the field type, which would emit one error per union arm per bad key.
    """
    if metadata is None:
        return None
    if len(metadata) > max_keys:
        raise ValueError(f"metadata exceeds {max_keys} keys (got {len(metadata)})")
    for key, value in metadata.items():
        if not KEY_PATTERN.match(key):
            raise ValueError(f"metadata key {key!r} must match {KEY_PATTERN.pattern}")
        if not isinstance(value, (str, int, float, bool)):
            raise ValueError(f"metadata value for key {key!r} must be str/int/float/bool, got {type(value).__name__}")
        if isinstance(value, str) and len(value) > MAX_VALUE_LEN:
            raise ValueError(f"metadata value for key {key!r} exceeds {MAX_VALUE_LEN} characters")
    return metadata
