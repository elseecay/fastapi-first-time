from typing import Optional, List, Tuple
from contextlib import closing
from datetime import datetime, timezone, timedelta

import sqlalchemy

from sqlalchemy import select, insert, delete, and_

from models import models


def execute_sql(conn: sqlalchemy.Connection, sql_stmt, *, close_cursor=False, fetch_one=False):
    cursor = conn.execute(sql_stmt)
    if fetch_one:
        row = cursor.fetchone()
        cursor.close()
        return row
    if close_cursor:
        cursor.close()
        return None
    return cursor


def persons_get_by_guid(conn, guid, *, retid=False) -> Optional[sqlalchemy.Row | int]:
    stmt = select(models.persons).where(models.persons.c.guid == guid)
    row = execute_sql(conn, stmt, fetch_one=True)
    if row is None:
        return None
    if retid:
        return row.id
    return row


def persons_insert(conn, guid, name) -> int:
    stmt = insert(models.persons).values(guid=guid, name=name).returning(models.persons.c.id)
    row = execute_sql(conn, stmt, fetch_one=True)
    return row.id


def databases_get_by_name(conn, name, *, retid=False) -> Optional[int]:
    stmt = select(models.databases.c.id).where(models.databases.c.name == name)
    row = execute_sql(conn, stmt, fetch_one=True)
    if row is None:
        return None
    if retid:
        return row.id
    return row


def databases_get_all_names(conn) -> List[str]:
    stmt = select(models.databases.c.name)
    cursor = execute_sql(conn, stmt)
    with closing(cursor):
        return list(row.name for row in cursor.fetchall())


def stats_insert(conn, person_id, database_id, dcount, ccount, hindex, url) -> int:
    insert_args = {
        "person_id": person_id,
        "database_id": database_id,
        "dcount": dcount,
        "ccount": ccount,
        "hindex": hindex,
        "url": url,
        "creationtime": datetime.now(timezone(timedelta(0)))
    }
    stmt = insert(models.stats).values(**insert_args).returning(models.stats.c.id)
    row = execute_sql(conn, stmt, fetch_one=True)
    return row.id


def stats_remove(conn, person_id, database_id):
    cond = and_(models.stats.c.person_id == person_id, models.stats.c.database_id == database_id)
    stmt = delete(models.stats).where(cond)
    execute_sql(conn, stmt, close_cursor=True)


def stats_get(conn, person_id, database_id) -> Optional[sqlalchemy.Row]:
    cond = and_(models.stats.c.person_id == person_id, models.stats.c.database_id == database_id)
    stmt = select(models.stats).where(cond)
    row = execute_sql(conn, stmt, fetch_one=True)
    return row
    