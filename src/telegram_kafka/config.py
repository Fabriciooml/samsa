"""Load database URL for migration CLI from env or mounted secret."""

import os
from pathlib import Path


def database_url() -> str:
    if os.getenv("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    if os.getenv("DATABASE_URL_FILE"):
        return Path(os.environ["DATABASE_URL_FILE"]).read_text().rstrip("\r\n")
    raise ValueError("Missing DATABASE_URL or DATABASE_URL_FILE")
