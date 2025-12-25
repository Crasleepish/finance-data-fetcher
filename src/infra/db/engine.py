from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import Connection, Engine, create_engine

from config.settings import DatabaseConfig


def create_engine_from_config(config: DatabaseConfig) -> Engine:
    return create_engine(config.url, pool_pre_ping=True)


@contextmanager
def transaction(engine: Engine) -> Iterator[Connection]:
    connection = engine.connect()
    trans = connection.begin()
    try:
        yield connection
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        connection.close()
