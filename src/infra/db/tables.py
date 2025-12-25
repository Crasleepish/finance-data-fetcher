from __future__ import annotations

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, func

metadata = MetaData()

test_messages = Table(
    "test_messages",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("message", String(length=255), nullable=False),
    Column(
        "update_time",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    ),
)
