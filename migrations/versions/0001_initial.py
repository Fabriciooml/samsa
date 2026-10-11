"""Initial bot, route, and delivery tables.

Revision ID: 0001
Revises:
"""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "admin_user",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("password_hash", sa.Text(), nullable=False),
    )
    op.create_table(
        "bot",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("token_encrypted", sa.Text(), nullable=False),
        sa.Column("token_hint", sa.String(16), nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "uq_bot_active_name", "bot", ["name"], unique=True,
        postgresql_where=sa.text("archived_at IS NULL"),
    )
    op.create_table(
        "route",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("topic", sa.String(249), nullable=False),
        sa.Column("bot_id", sa.String(36), sa.ForeignKey("bot.id"), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("template", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "route_start_offset",
        sa.Column("route_id", sa.String(36), sa.ForeignKey("route.id"), primary_key=True),
        sa.Column("partition", sa.Integer(), primary_key=True),
        sa.Column("start_offset", sa.BigInteger(), nullable=False),
    )
    op.create_table(
        "delivery_failure",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("route_id", sa.String(36), sa.ForeignKey("route.id"), nullable=False),
        sa.Column("topic", sa.String(249), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("offset", sa.BigInteger(), nullable=False),
        sa.Column("category", sa.String(60), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
        sa.Column("rendered_text", sa.Text()),
        sa.Column("retry_state", sa.String(30), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("route_id", "topic", "partition", "offset", "category"),
    )


def downgrade() -> None:
    op.drop_table("delivery_failure")
    op.drop_table("route_start_offset")
    op.drop_table("route")
    op.drop_index("uq_bot_active_name", table_name="bot")
    op.drop_table("bot")
    op.drop_table("admin_user")
