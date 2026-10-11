#!/bin/sh
set -eu
if [ "${AUTO_GENERATE_KEYS:-false}" = "true" ]; then
  : "${KEYS_DIR:=/app/keys}"
  python -m telegram_kafka.bootstrap_keys "$KEYS_DIR"
  export TOKEN_ENCRYPTION_KEY_FILE="$KEYS_DIR/token_key"
  export SESSION_SECRET_FILE="$KEYS_DIR/session_secret"
fi
alembic upgrade head
exec uvicorn telegram_kafka.app:create_app --factory --host 0.0.0.0 --port 8000 --workers 1
