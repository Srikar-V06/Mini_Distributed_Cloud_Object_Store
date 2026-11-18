import json
from errors import ApiError

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