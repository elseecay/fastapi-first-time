from typing import Annotated, List, Literal, Optional
from enum import Enum
from contextlib import closing

from sqlalchemy import select, func
from pydantic import BaseModel, HttpUrl
from fastapi import FastAPI, APIRouter, Query, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

import crud

from crud import execute_sql
from database import engine
from models import models


with engine.connect() as conn:
    ScienceDatabase = Enum("ScienceDatabase", [(name, name) for name in crud.databases_get_all_names(conn)])


class ErrorCode(int, Enum):
    ERR_UNKNOWN = 0
    ERR_VALIDATION = 1
    ERR_RECORD_NOT_FOUND = 2


class ErrorModel(BaseModel):
    error_code: int
    error_code_text: str
    error_text: str

    def to_json_response(self, status_code: int):
        return JSONResponse({"error_code": self.error_code, "error_code_text": self.error_code_text, "error_text": self.error_text}, status_code=status_code)


class CustomHttpException(HTTPException):
    
    def __init__(self, error_code: ErrorCode, error_text: str, status_code: int = 400):
        super().__init__(status_code=status_code)
        self.error_code = error_code
        self.error_text = error_text

    def to_error_model(self):
        code = self.error_code.value
        code_text = str(self.error_code).replace(ErrorCode.__name__, "").replace(".", "")
        return ErrorModel(error_code=code, error_code_text=code_text, error_text=self.error_text)

    def __repr__(self):
        return self.error_text


app = FastAPI(responses={400: {"model": ErrorModel}})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc) -> JSONResponse:
    exc = CustomHttpException(error_code=ErrorCode.ERR_VALIDATION, error_text=str(exc), status_code=400)
    return exc.to_error_model().to_json_response(exc.status_code)


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc) -> JSONResponse:
    exc = CustomHttpException(error_code=ErrorCode.ERR_UNKNOWN, error_text=str(exc), status_code=400)
    return exc.to_error_model().to_json_response(exc.status_code)


@app.exception_handler(CustomHttpException)
async def http_exception_handler(request, exc: CustomHttpException) -> JSONResponse:
    return exc.to_error_model().to_json_response(exc.status_code)


class AddProfileResponse(BaseModel):
    internal_id: int


@app.post("/profile", responses={200: {"model": AddProfileResponse}})
def add_profile(
    guid: Annotated[str, Query(regex=r"[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}", description="16 byte hexadecimal GUID")],
    name: Annotated[str, Query(max_length=100)],
    db: ScienceDatabase,
    dcount: Annotated[int, Query(ge=0)],
    ccount: Annotated[int, Query(ge=0)],
    hindex: Annotated[int, Query(ge=0)],
    url: HttpUrl
):
    with engine.begin() as conn:
        db_id = crud.databases_get_by_name(conn, db.value, retid=True)
        person_id = crud.persons_get_by_guid(conn, guid, retid=True)
        if person_id is None:
            person_id = crud.persons_insert(conn, guid, name)
        if crud.stats_get(conn, person_id, db_id) is not None:
            crud.stats_remove(conn, person_id, db_id)
        internal_id = crud.stats_insert(conn, person_id, db_id, dcount, ccount, hindex, url)
    return AddProfileResponse(internal_id=internal_id)


class GetProfileResponse(BaseModel):
    name: str
    hindex: int
    url: str
    dcount: Optional[int] = None
    ccount: Optional[int] = None


@app.get("/profile", responses={200: {"model": GetProfileResponse}})
def get_profile(
    guid: Annotated[str, Query(regex=r"[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}", description="16 byte GUID string")],
    db: ScienceDatabase,
    fields: Annotated[List[Literal["dcount", "ccount"]], Query()] = None
):
    fields = tuple() if fields is None else fields
    with engine.connect() as conn:
        db_id = crud.databases_get_by_name(conn, db.value, retid=True)
        person_row = crud.persons_get_by_guid(conn, guid)
        if person_row is None:
            raise CustomHttpException(ErrorCode.ERR_RECORD_NOT_FOUND, "Unknown person GUID")
        stats_row = crud.stats_get(conn, person_row.id, db_id)
        if stats_row is None:
            raise CustomHttpException(ErrorCode.ERR_RECORD_NOT_FOUND, "Record not found for pair [GUID, DB]")
    response = GetProfileResponse(name=person_row.name, hindex=stats_row.hindex, url=stats_row.url)
    response.dcount = stats_row.dcount if "dcount" in fields else None
    response.ccount = stats_row.ccount if "ccount" in fields else None
    return response


class GetAllProfilesResponseItem(BaseModel):
    name: str
    hindex: int
    url: str


@app.get("/all_profiles", responses={200: {"model": List[GetAllProfilesResponseItem]}})
def get_all_profiles(
    db: ScienceDatabase,
    page: Annotated[int, Query(ge=0)] = 0,
    sfield: Literal["hindex", "creation_time"] = "hindex",
    sorder: Literal["asc", "desc"] = "asc"
):
    with engine.connect() as conn:
        db_id = crud.databases_get_by_name(conn, db.value, retid=True)
        stmt = build_query_persons_list(db_id, page, sfield, sorder)
        with closing(execute_sql(conn, stmt)) as cursor:
            rows = cursor.fetchall()
    return [GetAllProfilesResponseItem(name=row.name, hindex=row.hindex, url=row.url) for row in rows]


class GetStatisticsResponseItem(BaseModel):
    database: str
    sum_dcount: int
    sum_ccount: int
    avg_hindex: int


@app.get("/statistics", responses={200: {"model": List[GetStatisticsResponseItem]}})
def get_statistics():
    stmt = build_query_stat()
    with engine.connect() as conn:
        with closing(execute_sql(conn, stmt)) as cursor:
            rows = cursor.fetchall()
    return [GetStatisticsResponseItem(database=row.name, sum_dcount=row.sum_dcount, sum_ccount=row.sum_ccount, avg_hindex=int(row.avg_hindex)) for row in rows]


def build_query_persons_list(db_id, page, sfield, sorder):
    stmt = select(models.persons.c.name, models.stats.c.hindex, models.stats.c.url)
    stmt = stmt.join_from(models.stats, models.persons).where(models.stats.c.database_id == db_id)
    sorting_col = models.stats.c.hindex if sfield == "hindex" else models.stats.c.creationtime
    sorting_col = sorting_col.asc() if sorder == "asc" else sorting_col.desc()
    stmt = stmt.order_by(sorting_col).offset(page * 10).limit(10)
    return stmt


def build_query_stat():
    sum_dcount = func.sum(models.stats.c.dcount).label("sum_dcount")
    sum_ccount = func.sum(models.stats.c.ccount).label("sum_ccount")
    avg_hindex = func.avg(models.stats.c.hindex).label("avg_hindex")
    stmt = select(models.databases.c.name, sum_dcount, sum_ccount, avg_hindex)
    stmt = stmt.join_from(models.stats, models.databases).group_by(models.databases.c.name)
    return stmt
