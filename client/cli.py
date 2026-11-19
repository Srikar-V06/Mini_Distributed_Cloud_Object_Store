"""Command-line client for the distributed object store."""

import argparse
import hashlib
import json
import math
import os
import re
import sys
import tempfile
from pathlib import Path

import httpx

OBJECTS = "/api/v1/objects"
BLOCK_SIZE = 1024 * 1024
TIMEOUT = httpx.Timeout(connect=5.0, read=600.0, write=60.0, pool=10.0)

# argparse reserves 2 for invalid command-line arguments.
LOCAL_ERROR = 3
INTEGRITY_ERROR = 4
NETWORK_ERROR = 5
NOT_FOUND = 6
REQUEST_ERROR = 7
UNAVAILABLE = 8
SERVER_ERROR = 9


class CliError(Exception):
    def __init__(self, exit_code: int, code: str, message: str, **details):
        super().__init__(message)
        self.exit_code = exit_code
        self.body = {"error": code, "message": message, **details}


def key_value(token: str):
    key, separator, raw = token.partition("=")
    if not separator or not key:
        raise argparse.ArgumentTypeError("metadata must use key=value with a nonempty key")
    try:
        value = json.loads(raw)
    except ValueError:
        value = raw
    if isinstance(value, (dict, list)) or isinstance(value, float) and not math.isfinite(value):
        raise argparse.ArgumentTypeError("metadata values must be strings, finite numbers, booleans, or null")
    return key, value


def object_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise argparse.ArgumentTypeError("object ID must contain only letters, digits, underscores, or hyphens")
    return value


def object_path(value: str) -> str:
    return f"{OBJECTS}/{value}"


def response_json(response: httpx.Response):
    response.raise_for_status()
    try:
        return response.json()
    except ValueError as exc:
        raise CliError(SERVER_ERROR, "invalid_response", "controller returned invalid JSON") from exc


def upload(client: httpx.Client, args: argparse.Namespace) -> dict:
    metadata = dict(args.meta)
    fields = {"metadata": json.dumps(metadata)} if metadata else None
    with args.file.open("rb") as source:
        response = client.post(OBJECTS, data=fields, files={"file": (args.file.name, source)})
    return response_json(response)


def download_headers(response: httpx.Response) -> tuple[int, str]:
    length = response.headers.get("Content-Length", "")
    checksum = response.headers.get("X-Object-Checksum", "")
    if not re.fullmatch(r"[0-9]+", length) or not re.fullmatch(r"[0-9a-fA-F]{64}", checksum):
        raise CliError(INTEGRITY_ERROR, "invalid_download_headers",
                       "download requires a valid Content-Length and X-Object-Checksum")
    try:
        return int(length), checksum.lower()
    except ValueError as exc:
        raise CliError(INTEGRITY_ERROR, "invalid_download_headers", "invalid Content-Length") from exc


def download(client: httpx.Client, args: argparse.Namespace) -> dict:
    temp_path = None
    try:
        with client.stream("GET", object_path(args.id), headers={"Accept-Encoding": "identity"}) as response:
            if not response.is_success:
                response.read()
                response.raise_for_status()
            if response.status_code != 200:
                raise CliError(INTEGRITY_ERROR, "invalid_download_status", "download requires HTTP 200")
            expected_size, expected_checksum = download_headers(response)
            digest = hashlib.sha256()
            size = 0
            with tempfile.NamedTemporaryFile(
                    dir=args.out.parent, prefix=".objectstore-", suffix=".part", delete=False) as target:
                temp_path = Path(target.name)
                try:
                    for block in response.iter_raw(chunk_size=BLOCK_SIZE):
                        size += len(block)
                        if size > expected_size:
                            raise CliError(INTEGRITY_ERROR, "length_mismatch",
                                           "download exceeded the declared Content-Length",
                                           expected_size=expected_size, received_size=size)
                        target.write(block)
                        digest.update(block)
                except httpx.RemoteProtocolError as exc:
                    raise CliError(INTEGRITY_ERROR, "incomplete_download", str(exc)) from exc

            checksum = digest.hexdigest()
            if size != expected_size:
                raise CliError(INTEGRITY_ERROR, "length_mismatch", "download size differs from Content-Length",
                               expected_size=expected_size, received_size=size)
            if checksum != expected_checksum:
                raise CliError(INTEGRITY_ERROR, "checksum_mismatch", "download SHA-256 differs from the controller",
                               expected_checksum=expected_checksum, received_checksum=checksum)

        temp_path.replace(args.out)
        temp_path = None
        return {"object_id": args.id, "path": str(args.out), "size": size, "checksum": checksum}
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def metadata(client: httpx.Client, args: argparse.Namespace) -> dict:
    return response_json(client.get(f"{object_path(args.id)}/metadata"))


def patch_metadata(client: httpx.Client, args: argparse.Namespace) -> dict:
    return response_json(client.patch(f"{object_path(args.id)}/metadata", json=dict(args.values)))


def list_objects(client: httpx.Client, args: argparse.Namespace) -> dict:
    return response_json(client.get(OBJECTS, params={"limit": args.limit, "offset": args.offset}))


def delete(client: httpx.Client, args: argparse.Namespace) -> dict:
    client.delete(object_path(args.id)).raise_for_status()
    return {"object_id": args.id, "deleted": True}


def file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(BLOCK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def verify(original: Path, downloaded: Path) -> dict:
    original_checksum = file_checksum(original)
    downloaded_checksum = file_checksum(downloaded)
    result = {"original_checksum": original_checksum, "downloaded_checksum": downloaded_checksum}
    if original_checksum != downloaded_checksum:
        raise CliError(INTEGRITY_ERROR, "checksum_mismatch", "the two files have different SHA-256 hashes",
                       **result)
    return {"match": True, **result}


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--base", default=os.getenv("OBJECT_STORE_URL", "http://localhost:8000"),
                     help="controller URL (default: OBJECT_STORE_URL or http://localhost:8000)")
    commands = cli.add_subparsers(dest="command", required=True)

    cmd = commands.add_parser("upload", help="upload a local file")
    cmd.add_argument("file", type=Path)
    cmd.add_argument("--meta", nargs="+", action="extend", type=key_value, default=[],
                     metavar="KEY=VALUE", help="custom metadata; accepts text and JSON scalars")
    cmd.set_defaults(run=upload)

    cmd = commands.add_parser("download", help="download and verify an object")
    cmd.add_argument("id", type=object_id)
    cmd.add_argument("out", type=Path)
    cmd.set_defaults(run=download)

    cmd = commands.add_parser("meta", help="show object metadata")
    cmd.add_argument("id", type=object_id)
    cmd.set_defaults(run=metadata)

    cmd = commands.add_parser("patch", help="update metadata; use KEY=null to remove a key")
    cmd.add_argument("id", type=object_id)
    cmd.add_argument("values", nargs="+", type=key_value, metavar="KEY=VALUE")
    cmd.set_defaults(run=patch_metadata)

    cmd = commands.add_parser("list", help="list completed objects")
    cmd.add_argument("--limit", type=int, default=100)
    cmd.add_argument("--offset", type=int, default=0)
    cmd.set_defaults(run=list_objects)

    cmd = commands.add_parser("delete", help="delete an object")
    cmd.add_argument("id", type=object_id)
    cmd.set_defaults(run=delete)

    cmd = commands.add_parser("verify", help="compare the SHA-256 hashes of two local files")
    cmd.add_argument("original", type=Path)
    cmd.add_argument("downloaded", type=Path)
    return cli


def http_error(exc: httpx.HTTPStatusError) -> CliError:
    response = exc.response
    status = response.status_code
    if status == 404:
        exit_code = NOT_FOUND
    elif status in (502, 503, 504):
        exit_code = UNAVAILABLE
    elif status >= 500:
        exit_code = SERVER_ERROR
    else:
        exit_code = REQUEST_ERROR

    result = CliError(exit_code, "http_error", f"controller returned HTTP {status} {response.reason_phrase}")
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        result.body.update(body)
    if request_id := response.headers.get("X-Request-ID"):
        result.body.setdefault("request_id", request_id)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "verify":
            result = verify(args.original, args.downloaded)
        else:
            with httpx.Client(base_url=args.base.rstrip("/"), timeout=TIMEOUT) as client:
                result = args.run(client, args)
    except httpx.HTTPStatusError as exc:
        error = http_error(exc)
    except (httpx.RequestError, httpx.InvalidURL) as exc:
        error = CliError(NETWORK_ERROR, "request_failed", str(exc))
    except FileNotFoundError as exc:
        error = CliError(LOCAL_ERROR, "file_not_found", f"local path not found: {exc.filename}")
    except OSError as exc:
        error = CliError(LOCAL_ERROR, "file_error", str(exc))
    except CliError as exc:
        error = exc
    else:
        print(json.dumps(result, indent=2))
        return 0

    print(json.dumps(error.body, indent=2), file=sys.stderr)
    return error.exit_code


if __name__ == "__main__":
    sys.exit(main())
