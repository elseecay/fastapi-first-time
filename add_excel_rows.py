import pandas
import sqlalchemy
import sqlalchemy.exc

from models import models


excel_data = pandas.read_excel("dataset.xlsx")
engine = sqlalchemy.create_engine("sqlite:///science.db")


with engine.connect() as dbcon:

    dbcon.execute(models.stats.delete().where(True))
    dbcon.execute(models.persons.delete().where(True))
    dbcon.execute(models.databases.delete().where(True))

    for i in range(excel_data.shape[0]):
        row = excel_data.iloc[i]

        insert_params = {
            "name": row[1],
            "guid": row[0],
        }
        sql_stmt = models.persons.insert().values(**insert_params)
        try:
            dbcon.execute(sql_stmt)
        except sqlalchemy.exc.IntegrityError:
            pass

        insert_params = {
            "name": row[2]
        }
        sql_stmt = models.databases.insert().values(**insert_params)
        try:
            dbcon.execute(sql_stmt)
        except sqlalchemy.exc.IntegrityError:
            pass

        sql_stmt = models.persons.select().where(models.persons.c.guid == row["guid"])
        person_id = dbcon.execute(sql_stmt).fetchone()[0]
        sql_stmt = models.databases.select().where(models.databases.c.name == row[2])
        database_id = dbcon.execute(sql_stmt).fetchone()[0]
        insert_params = {
            "person_id": person_id,
            "database_id": database_id,
            "dcount": int(row[3]),
            "ccount": int(row[4]),
            "hindex": int(row[5]),
            "url": row[6]
        }
        sql_stmt = models.stats.insert().values(**insert_params)
        dbcon.execute(sql_stmt)
        dbcon.commit()
