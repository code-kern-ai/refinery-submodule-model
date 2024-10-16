import os
from typing import Tuple, Any, Union, List, Dict, Optional, Iterable
from pydantic import BaseModel
import collections
from re import sub, match, compile
import sqlalchemy
from uuid import UUID
from datetime import datetime


from sqlalchemy.sql import text as sql_text
from sqlalchemy.engine.row import Row
from .models import Base
from .business_objects import general

CAMEL_CASE_PATTERN = compile(r"^([a-z]+[A-Z]?)*$")


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
# whitelist works for both row objects and class objects
# afaik only class objects "benefit" from the reduced amount of data selected as the row object selects beforehand, best to build the select directly
def sql_alchemy_to_dict(
    sql_alchemy_object: Any,
    for_frontend: bool = False,
    column_whitelist: Optional[Iterable[str]] = None,
):
    result = __sql_alchemy_to_dict(sql_alchemy_object, column_whitelist)
    if for_frontend:
        return to_frontend_obj(result)
    return result


def pack_edges_node(result, name: str, max_lvl: Optional[int] = None):

    def convert_value(value, max_lvl: int):
        new_lvl = max_lvl - 1 if max_lvl is not None else None
        if isinstance(value, list):
            return {
                "edges": [
                    {
                        "node": (
                            convert_value(item, new_lvl)
                            if max_lvl is None or max_lvl > 0
                            else item
                        )
                    }
                    for item in value
                ]
            }
        elif isinstance(value, dict):
            return {
                key: (
                    convert_value(val, new_lvl)
                    if max_lvl is None or max_lvl > 0
                    else val
                )
                for key, val in value.items()
            }
        else:
            return value

    return {"data": {name: convert_value(result, max_lvl)}}


def __sql_alchemy_to_dict(
    sql_alchemy_object: Any, column_whitelist: Optional[Iterable[str]] = None
):
    if isinstance(sql_alchemy_object, list):
        # list is for all() queries
        return [__sql_alchemy_to_dict(x, column_whitelist) for x in sql_alchemy_object]

    elif isinstance(sql_alchemy_object, Row):
        # basic SELECT .. FROM query)
        # _mapping is a RowMapping object that is not serializable but dict like
        return {
            k: v
            for k, v in dict(sql_alchemy_object._mapping).items()
            if not column_whitelist or k in column_whitelist
        }
    elif isinstance(sql_alchemy_object, Base):
        return {
            c.name: getattr(sql_alchemy_object, c.name)
            for c in sql_alchemy_object.__table__.columns
            if not column_whitelist or c.name in column_whitelist
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


def to_frontend_obj_raw(value: Union[List, Dict]):
    if isinstance(value, dict):
        return {k: to_frontend_obj_raw(v) for k, v in value.items()}
    elif is_list_like(value):
        return [to_frontend_obj_raw(x) for x in value]
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
    if is_camel_case(name):
        return name
    name = sub(r"(_|-)+", " ", name).title().replace(" ", "")
    return "".join([name[0].lower(), name[1:]])


def is_list_like(value: Any) -> bool:
    return (
        isinstance(value, collections.Iterable)
        and not isinstance(value, str)
        and not isinstance(value, dict)
        and not isinstance(value, Row)
        and not isinstance(value, BaseModel)
    )


def is_camel_case(text: str) -> bool:
    if match(CAMEL_CASE_PATTERN, text):
        return True
    else:
        return False


# str is expected but depending on the attack vector e.g. the type hints don't mean anything so an int could still receive a string
# the idea is that every directly inserted variable (e.g. project_id) is run through this function before being used in a plain text query
# orm model is sufficient for most cases but for raw queries we mask all directly included variables
def prevent_sql_injection(variable_value: Union[str, Any], remove_quotes: bool) -> str:
    # Example usage, note that some_int is e.g. typed as int but sql injection attack only works with a string.
    # Type checks are already done by fastapi but to ensure there aren't any issues with faulty type hints we do a check here as well
    # some_str = prevent_sql_injection(some_str, isinstance(some_str, str))
    # some_int = prevent_sql_injection(some_int, isinstance(some_str, int))
    if variable_value is None:
        return variable_value

    if isinstance(variable_value, str):
        return __mask_sql_str(variable_value, remove_quotes)
    elif isinstance(variable_value, list):
        return [__mask_sql_str(x, remove_quotes) for x in variable_value]
    elif isinstance(variable_value, dict):
        return {k: __mask_sql_str(v, remove_quotes) for k, v in variable_value.items()}
    return variable_value


def __mask_sql_str(sql_str: str, remove_quotes: bool) -> str:
    if not isinstance(sql_str, str):
        raise ValueError("sql_str is not a string")

    value = sqlalchemy.String("").literal_processor(dialect=general.get_dialect())(
        value=sql_str
    )

    if remove_quotes:
        return value[1:-1]
    return value


def ensure_sql_text(sql: str) -> str:
    return sql_text(sql)
