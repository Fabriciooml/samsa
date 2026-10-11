from pathlib import Path

from cryptography.fernet import Fernet

from telegram_kafka.bootstrap_keys import ensure_keys


def test_bootstrap_keys_are_valid_private_and_stable(tmp_path: Path):
    ensure_keys(tmp_path)
    token_key = (tmp_path / "token_key").read_text()
    session_secret = (tmp_path / "session_secret").read_text()
    Fernet(token_key.encode())
    assert len(session_secret) >= 48
    assert (tmp_path / "token_key").stat().st_mode & 0o777 == 0o600
    assert (tmp_path / "session_secret").stat().st_mode & 0o777 == 0o600

    ensure_keys(tmp_path)
    assert (tmp_path / "token_key").read_text() == token_key
    assert (tmp_path / "session_secret").read_text() == session_secret
