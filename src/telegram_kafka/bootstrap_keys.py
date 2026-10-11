"""Create persistent application keys for browser-only Portainer deployment."""

from __future__ import annotations

import os
import secrets
import sys
from pathlib import Path

from cryptography.fernet import Fernet


def _create_once(path: Path, value: str) -> None:
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return
    with os.fdopen(descriptor, "w") as file:
        file.write(value)


def ensure_keys(directory: Path) -> None:
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    _create_once(directory / "token_key", Fernet.generate_key().decode())
    _create_once(directory / "session_secret", secrets.token_urlsafe(48))


if __name__ == "__main__":
    ensure_keys(Path(sys.argv[1]))
