import os
from typing import Tuple, Any, Union, List, Dict
import collections
from re import sub

from uuid import UUID
from datetime import datetime

from sqlalchemy.engine.row import Row
from .models import Base


def collect_engine_variables() -> Tuple[int, int, bool, bool]:
    # amount of simultaneous connections to the database
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_size
    pool_size = 20
    os_pool_size = os.getenv("POSTGRES_POOL_SIZE")
    if os_pool_size:
        try:
            pool_size = int(os_pool_size)
        except ValueError:
            print(
                f"POSTGRES_POOL_SIZE is not an integer, using default {pool_size}",
                flush=True,
            )
    # Recycle connections after x seconds. This is only done on checkout not "always"
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_recycle
    # https://docs.sqlalchemy.org/en/20/core/pooling.html#setting-pool-recycle
    pool_recycle = 3600
    os_pool_recycle = os.getenv("POSTGRES_POOL_RECYCLE")
    if os_pool_recycle:
        try:
            pool_recycle = int(os_pool_recycle)
        except ValueError:
            print(
                f"POSTGRES_POOL_RECYCLE is not an integer, using default {pool_recycle}",
                flush=True,
            )
    # use LIFO instead of FIFO (stack vs queue)
    # https://docs.sqlalchemy.org/en/20/core/pooling.html#using-fifo-vs-lifo
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_use_lifo
    pool_use_lifo = False
    os_pool_use_lifo = os.getenv("POSTGRES_POOL_USE_LIFO")
    if os_pool_use_lifo:
        try:
            pool_use_lifo = os_pool_use_lifo.lower() in ["true", "x", "1", "y"]
        except ValueError:
            print(
                f"POSTGRES_POOL_USE_LIFO is not an boolean, using default {pool_use_lifo}",
                flush=True,
            )

    # test connections on checkout - results in a ping to the database (so small overhead per request) but ensures that the connection is still alive
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_pre_ping
    # https://docs.sqlalchemy.org/en/20/core/pooling.html#disconnect-handling-pessimistic
    pool_pre_ping = True
    os_pool_pre_ping = os.getenv("POSTGRES_POOL_PRE_PING")
    if os_pool_pre_ping:
        try:
            pool_pre_ping = os_pool_pre_ping.lower() in ["true", "x", "1", "y"]
        except ValueError:
            print(
                f"POSTGRES_POOL_PRE_PING is not an boolean, using default {pool_pre_ping}",
                flush=True,
            )

    # overflow of pool limit, -1 = infinite (shouldn't be used)
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.max_overflow
    # https://docs.sqlalchemy.org/en/20/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow

    pool_max_overflow = 10
    os_pool_max_overflow = os.getenv("POSTGRES_POOL_MAX_OVERFLOW")
    if os_pool_max_overflow:
        try:
            pool_max_overflow = int(os_pool_max_overflow)
        except ValueError:
            print(
                f"POSTGRES_POOL_MAX_OVERFLOW is not an integer, using default {pool_max_overflow}",
                flush=True,
            )

    return pool_size, pool_max_overflow, pool_recycle, pool_use_lifo, pool_pre_ping


# Row object is with a common SELECT query
# otherwise it's e.g. a Class Object (instance of Base)
def sql_alchemy_to_dict(sql_alchemy_object: Any, for_frontend: bool = False):
    result = __sql_alchemy_to_dict(sql_alchemy_object)
    if for_frontend:
        return to_frontend_obj(result)
    return result


def __sql_alchemy_to_dict(sql_alchemy_object: Any):
    if isinstance(sql_alchemy_object, list):
        # list is for all() queries
        return [__sql_alchemy_to_dict(x) for x in sql_alchemy_object]

    elif isinstance(sql_alchemy_object, Row):
        # basic SELECT .. FROM query)
        # _mapping is a RowMapping object that is not serializable but dict like
        return dict(sql_alchemy_object._mapping)
    elif isinstance(sql_alchemy_object, Base):
        return {
            c.name: getattr(sql_alchemy_object, c.name)
            for c in sql_alchemy_object.__table__.columns
        }
    else:
        return sql_alchemy_object


def to_frontend_obj(value: Union[List, Dict]):
    if isinstance(value, dict):
        return {__to_camel_case(k): to_frontend_obj(v) for k, v in value.items()}
    elif is_list_like(value):
        return [to_frontend_obj(x) for x in value]
    else:
        return __to_json_serializable(value)


def __to_json_serializable(x: Any):
    if isinstance(x, datetime):
        return x.isoformat()
    elif isinstance(x, UUID):
        return str(x)
    else:
        return x


def __to_camel_case(name: str):
    name = sub(r"(_|-)+", " ", name).title().replace(" ", "")
    return "".join([name[0].lower(), name[1:]])


def is_list_like(value: Any) -> bool:
    return (
        isinstance(value, collections.Iterable)
        and not isinstance(value, str)
        and not isinstance(value, dict)
        and not isinstance(value, Row)
    )
