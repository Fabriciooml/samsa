"""Package allowlisted application files for Portainer's browser image builder."""

from pathlib import Path
import tarfile


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "samsa-source.tar.gz"
FILES = [
    ROOT / "pyproject.toml",
    ROOT / "README.md",
    ROOT / "alembic.ini",
    ROOT / "docker-entrypoint.sh",
    *sorted((ROOT / "assets").rglob("*.webp")),
    *sorted((ROOT / "src").rglob("*.py")),
    *sorted((ROOT / "src").rglob("*.html")),
    *sorted((ROOT / "migrations").rglob("*.py")),
]

with tarfile.open(OUTPUT, "w:gz") as archive:
    for path in FILES:
        archive.add(path, arcname=path.relative_to(ROOT))

print(OUTPUT)
