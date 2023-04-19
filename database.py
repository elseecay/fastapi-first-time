import sqlite3

import sqlalchemy
import sqlalchemy.pool


__all__ = [
    "engine"
]


DATABASE_URL = "sqlite:///science.db"


def create_connection():
    connection = sqlite3.connect(DATABASE_URL[10:], check_same_thread=False, isolation_level="DEFERRED")
    connection.execute("PRAGMA foreign_keys = ON").close()
    return connection


engine: sqlalchemy.Engine = sqlalchemy.create_engine(DATABASE_URL, pool=sqlalchemy.pool.QueuePool(create_connection))
