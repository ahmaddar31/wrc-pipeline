from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_file(path: str | Path, chunk_size: int = 8192) -> str:
    file_path = Path(path)
    hasher = hashlib.sha256()

    with file_path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)

    return hasher.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()