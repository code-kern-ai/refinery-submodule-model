import os
from contextvars import ContextVar
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import PendingRollbackError
import traceback

request_id_ctx_var = ContextVar("request_id", default=None)


def get_request_id():
    return request_id_ctx_var.get()


engine = create_engine(os.getenv("POSTGRES"), pool_size=20)
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
