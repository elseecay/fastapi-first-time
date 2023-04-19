from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, String, DateTime, UniqueConstraint


__all__ = [
    "metadata",
    "persons",
    "databases",
    "stats"
]


metadata = MetaData()


persons = Table(
    "persons",
    metadata,
    
    Column("id", Integer, primary_key=True),
    Column("guid", String, unique=True, nullable=False),
    Column("name", String, nullable=False)
)


databases = Table(
    "databases",
    metadata,
    
    Column("id", Integer, primary_key=True),
    Column("name", String, unique=True, nullable=False)
)


stats = Table(
    "stats",
    metadata,

    Column("id", Integer, primary_key=True),
    Column("person_id", Integer, ForeignKey("persons.id")),
    Column("database_id", Integer, ForeignKey("databases.id")),
    Column("dcount", Integer),
    Column("ccount", Integer),
    Column("hindex", Integer),
    Column("url", String),
    Column("creationtime", DateTime, nullable=False),

    UniqueConstraint("person_id", "database_id", name="person_database_unique_constraint")
)
