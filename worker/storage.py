import errno
import hashlib
import os
import re
import secrets
import time
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CHUNK_DIR = DATA_DIR / "chunks"
TMP_DIR = DATA_DIR / "tmp"
MAX_CHUNK_BYTES = int(os.getenv("MAX_CHUNK_BYTES", 1024 * 1024))

ID_RE = re.compile(r"[A-Za-z0-9_-]+")
HEX64 = re.compile(r"[0-9a-f]{64}")


class ChunkMissing(Exception): ...
class ChunkCorrupt(Exception): ...
class ChecksumMismatch(Exception): ...
class DiskFull(Exception): ...


def valid_id(chunk_id: str) -> bool:
    return len(chunk_id) <= 200 and ID_RE.fullmatch(chunk_id) is not None


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def startup(worker_id: str) -> None:
    """Identity check first, then create dirs and wipe leftover temp files."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    id_file = DATA_DIR / "worker_id"
    if id_file.exists():
        found = id_file.read_text().strip()
        if found != worker_id:
            raise RuntimeError(
                f"Wrong volume: {id_file} says '{found}' but WORKER_ID is '{worker_id}'"
            )
    else:
        with open(id_file, "w") as f:
            f.write(worker_id)
            f.flush()
            os.fsync(f.fileno())
    CHUNK_DIR.mkdir(exist_ok=True)
    TMP_DIR.mkdir(exist_ok=True)
    for p in TMP_DIR.iterdir():
        p.unlink(missing_ok=True)


def store_chunk(chunk_id: str, data: bytes, expected_checksum: str) -> str:
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_checksum.strip().lower():
        raise ChecksumMismatch()

    suffix = secrets.token_hex(4)
    tmp_data = TMP_DIR / f"{chunk_id}.{suffix}"
    tmp_side = TMP_DIR / f"{chunk_id}.{suffix}.sha256"
    final = CHUNK_DIR / chunk_id
    side = CHUNK_DIR / f"{chunk_id}.sha256"
    try:
        with open(tmp_data, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        with open(tmp_side, "w") as f:
            f.write(actual)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_data, final)   # atomic, overwrites existing (idempotent re-PUT)
        os.replace(tmp_side, side)
        _fsync_dir(CHUNK_DIR)
    except OSError as e:
        tmp_data.unlink(missing_ok=True)
        tmp_side.unlink(missing_ok=True)
        if e.errno == errno.ENOSPC:
            raise DiskFull() from e
        raise
    return actual


def read_verified(chunk_id: str) -> tuple[bytes, str]:
    """Return (bytes, checksum) only if the chunk is fully intact."""
    path = CHUNK_DIR / chunk_id
    side = CHUNK_DIR / f"{chunk_id}.sha256"
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        raise ChunkMissing()
    if size == 0 or size > MAX_CHUNK_BYTES:
        raise ChunkCorrupt(f"bad size {size}")
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        raise ChunkMissing()
    try:
        recorded = side.read_text().strip()
    except (FileNotFoundError, UnicodeDecodeError):
        raise ChunkCorrupt("sidecar missing or unreadable")
    if not HEX64.fullmatch(recorded):
        raise ChunkCorrupt("sidecar garbled")
    if hashlib.sha256(data).hexdigest() != recorded:
        raise ChunkCorrupt("hash mismatch")
    return data, recorded


def delete_chunk(chunk_id: str) -> None:
    (CHUNK_DIR / chunk_id).unlink(missing_ok=True)
    (CHUNK_DIR / f"{chunk_id}.sha256").unlink(missing_ok=True)
    _fsync_dir(CHUNK_DIR)


def _chunk_files():
    with os.scandir(CHUNK_DIR) as it:
        return [e for e in it if e.is_file() and not e.name.endswith(".sha256")]


def list_chunks() -> list[dict]:
    """Checksum here is recomputed from the actual bytes, not read from the sidecar,
    so the controller's rejoin check (Step 11) can't be fooled by a good sidecar on bad data."""
    now = time.time()
    out = []
    for e in _chunk_files():
        try:
            st = e.stat()
            digest = hashlib.sha256(Path(e.path).read_bytes()).hexdigest()
        except FileNotFoundError:
            continue
        out.append({
            "chunk_id": e.name,
            "size": st.st_size,
            "checksum": digest,
            "mtime": st.st_mtime,
            "age_seconds": now - st.st_mtime,   # worker-side age, so no cross-clock comparison
        })
    return out


def stats() -> dict:
    files = _chunk_files()
    return {
        "chunk_count": len(files),
        "bytes_used": sum(e.stat().st_size for e in files),
    }


def scrub(sleep_between: float = 0.0, expected: dict[str, str] | None = None) -> dict:
    bad, checked = [], 0
    chunk_ids = expected if expected is not None else (e.name for e in _chunk_files())
    for chunk_id in chunk_ids:
        checked += 1
        try:
            _, digest = read_verified(chunk_id)
            if expected is not None and digest != expected[chunk_id]:
                raise ChunkCorrupt("hash differs from controller checksum")
        except ChunkCorrupt as ex:
            bad.append({"chunk_id": chunk_id, "state": "corrupt", "reason": str(ex)})
        except ChunkMissing:
            if expected is not None:
                bad.append({"chunk_id": chunk_id, "state": "lost", "reason": "file missing"})
        if sleep_between:
            time.sleep(sleep_between)
    return {"checked": checked, "bad": bad}
