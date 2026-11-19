"""Pipe into a Docker worker with python - CHUNK_ID MODE to damage a test replica."""

import argparse

import storage


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("chunk_id")
    parser.add_argument("mode", choices=("flip", "truncate", "append", "delete", "zero", "sidecar"))
    args = parser.parse_args()
    if not storage.valid_id(args.chunk_id):
        parser.error("invalid chunk ID")

    path = storage.CHUNK_DIR / args.chunk_id
    if not path.is_file() or path.stat().st_size == 0:
        parser.error(f"nonempty chunk not found: {args.chunk_id}")

    if args.mode == "flip":
        with path.open("r+b") as chunk:
            byte = chunk.read(1)[0]
            chunk.seek(0)
            chunk.write(bytes([byte ^ 1]))
    elif args.mode == "truncate":
        with path.open("r+b") as chunk:
            chunk.truncate(path.stat().st_size // 2)
    elif args.mode == "append":
        with path.open("ab") as chunk:
            chunk.write(b"corruption demo")
    elif args.mode == "delete":
        path.unlink()
    elif args.mode == "zero":
        path.write_bytes(bytes(path.stat().st_size))
    else:
        path.with_suffix(".sha256").write_text("invalid checksum\n")
    print(f"applied {args.mode} to {args.chunk_id}")


if __name__ == "__main__":
    main()
