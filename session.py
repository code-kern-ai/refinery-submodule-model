from typing import Any
import os
from contextvars import ContextVar
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import PendingRollbackError
import traceback
from .util import collect_engine_variables

request_id_ctx_var = ContextVar("request_id", default=None)


def get_request_id():
    return request_id_ctx_var.get()


(
    pool_size,
    pool_max_overflow,
    pool_recycle,
    pool_use_lifo,
    pool_pre_ping,
) = collect_engine_variables()


engine = create_engine(
    os.getenv("POSTGRES"),
    pool_size=pool_size,
    max_overflow=pool_max_overflow,
    pool_recycle=pool_recycle,
    pool_use_lifo=pool_use_lifo,
    pool_pre_ping=pool_pre_ping,
)

session = scoped_session(
    sessionmaker(autocommit=False, autoflush=True, bind=engine),
    scopefunc=get_request_id,
)

## uncomment following lines to enable db logging
""" import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

 """


def check_session_and_rollback():
    try:
        _ = session.connection()
    except PendingRollbackError:
        print("session issue detected, rollback initiated", flush=True)
        print(traceback.format_exc(), flush=True)
        while session.registry().in_transaction():
            session.rollback()


def get_engine_dialect() -> Any:
    if not engine:
        return None
    return engine.dialect
