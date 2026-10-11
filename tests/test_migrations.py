import os
import asyncio

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer


@pytest.mark.integration
def test_initial_migration_creates_operational_tables():
    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as postgres:
        url = postgres.get_connection_url()
        config = Config("alembic.ini")
        previous = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = url
        try:
            command.upgrade(config, "head")
            async def table_names():
                engine = create_async_engine(url)
                try:
                    async with engine.connect() as conn:
                        return await conn.run_sync(lambda sync_conn: set(inspect(sync_conn).get_table_names()))
                finally:
                    await engine.dispose()
            tables = asyncio.run(table_names())
            assert {"admin_user", "bot", "route", "route_start_offset", "delivery_failure"} <= tables
        finally:
            if previous is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = previous
