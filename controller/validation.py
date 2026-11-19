import json
from errors import ApiError
import re

def sanitize_filename(name: str | None) -> str:
    if not name:
        return "unnamed"
    name = name.replace("\\", "/").split("/")[-1]       # basename only -> kills ../../etc/passwd
    name = "".join(c for c in name if c.isprintable())  # strips control chars
    name = name.strip()[:255]
    return name if name not in ("", ".", "..") else "unnamed"

def parse_metadata(raw: str | None) -> dict:
    if raw is None or raw.strip() == "":
        return {}
    try:
        md = json.loads(raw)
    except json.JSONDecodeError:
        raise ApiError(400, "invalid_metadata", "metadata is not valid JSON")
    if not isinstance(md, dict):
        raise ApiError(400, "invalid_metadata", "metadata must be a JSON object")
    if len(md) > 50:
        raise ApiError(400, "invalid_metadata", "at most 50 metadata keys allowed")
    for k, v in md.items():
        if len(k) > 128:
            raise ApiError(400, "invalid_metadata", f"key too long: {k[:20]}...")
        if isinstance(v, (dict, list)):
            raise ApiError(400, "invalid_metadata", f"value for '{k}' must be a scalar")
        if isinstance(v, str) and len(v) > 1024:
            raise ApiError(400, "invalid_metadata", f"value for '{k}' exceeds 1024 chars")
    return md
ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

def is_valid_id(s: str) -> bool:
    return bool(ID_RE.fullmatch(s))
def parse_patch(raw: bytes) -> dict:
    """Validate a merge-patch body. null values are allowed here because they mean 'delete key'."""
    try:
        patch = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ApiError(400, "invalid_json", "request body is not valid JSON")
    if not isinstance(patch, dict):
        raise ApiError(400, "invalid_metadata", "body must be a JSON object")
    if len(patch) > 50:
        raise ApiError(400, "invalid_metadata", "at most 50 keys per request")
    for k, v in patch.items():
        if not k or len(k) > 128:
            raise ApiError(400, "invalid_metadata", "keys must be 1-128 characters")
        if isinstance(v, (dict, list)):
            raise ApiError(400, "invalid_metadata", f"value for '{k}' must be a scalar or null")
        if isinstance(v, str) and len(v) > 1024:
            raise ApiError(400, "invalid_metadata", f"value for '{k}' exceeds 1024 chars")
    return patch


def parse_nonneg_int(name: str, s: str, lo: int, hi: int) -> int:
    """Strict: digits only. Rejects '-1', '1.5', 'abc', '' with a 400 (we reject, not clamp)."""
    if not (s.isascii() and s.isdigit()):
        raise ApiError(400, "invalid_parameter", f"'{name}' must be a non-negative integer")
    n = int(s)
    if n < lo or n > hi:
        raise ApiError(400, "invalid_parameter", f"'{name}' must be between {lo} and {hi}")
    return n