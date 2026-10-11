FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY assets ./assets
COPY src ./src
RUN pip install --no-cache-dir .

COPY alembic.ini ./
COPY migrations ./migrations
COPY docker-entrypoint.sh ./
RUN chmod +x ./docker-entrypoint.sh && useradd --create-home --uid 10001 notifier && mkdir /app/keys && chown -R notifier:notifier /app

USER notifier
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=4)"
CMD ["./docker-entrypoint.sh"]
