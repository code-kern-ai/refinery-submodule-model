import uuid
from typing import Any, Dict, List, Optional, Union
from sqlalchemy.orm.session import make_transient as make_transient_original
from ..session import session, engine
from ..session import request_id_ctx_var
from ..session import check_session_and_rollback as check_and_roll
from ..enums import Tablenames, try_parse_enum_value
import traceback
import datetime
from .. import daemon
from threading import Lock


__THREAD_LOCK = Lock()

session_lookup = {}


def get_ctx_token() -> Any:
    global session_lookup
    session_uuid = str(uuid.uuid4())
    session_id = request_id_ctx_var.set(session_uuid)

    call_stack = "".join(traceback.format_stack()[-5:])
    with __THREAD_LOCK:
        session_lookup[session_uuid] = {
            "session_id": session_uuid,
            "stack": call_stack,
            "created_at": datetime.datetime.now(),
        }
    return session_id


def get_session_lookup(exclude_last_x_seconds: int = 5) -> Dict[str, Dict[str, Any]]:
    # every requests creates its own session and usually there are a lot of short running session because of requests open
    # since these usually aren't interesting we default filter for >5 seconds sessions
    # this will still include long running sessions (e.g. longs data collection) and errors but not the short lived ones (e.g. created for the request itself)

    now = datetime.datetime.now()
    return [
        session_data
        for session_data in session_lookup.values()
        if (now - session_data["created_at"]).seconds > exclude_last_x_seconds
    ]


def reset_ctx_token(
    ctx_token: Any,
    remove_db: Optional[bool] = False,
) -> None:
    if remove_db:
        session.remove()
    session_uuid = ctx_token.var.get()

    request_id_ctx_var.reset(ctx_token)
    global session_lookup
    with __THREAD_LOCK:
        if session_uuid in session_lookup:
            del session_lookup[session_uuid]
        else:
            print("Session not found in lookup", flush=True)


def force_remove_and_refresh_session_by_id(session_id: str) -> bool:
    global session_lookup
    with __THREAD_LOCK:
        if session_id not in session_lookup:
            return False
    # context vars cant be closed from a different context but we can work around it by using a thread (which creates a new context) with the same id
    daemon.run_without_db_token(__close_in_context(session_id))
    return True


def __close_in_context(session_uuid: str):
    # set to same id so the get function used in session.get_request_id() returns the same id
    session_id = request_id_ctx_var.set(session_uuid)
    # remove connected session
    session.remove()
    # reset context variable
    request_id_ctx_var.reset(session_id)
    # remove from lookup
    global session_lookup
    with __THREAD_LOCK:
        del session_lookup[session_uuid]


def add(entity: Any, with_commit: bool = False) -> None:
    session.add(entity)
    flush_or_commit(with_commit)


def add_all(entities: List[Any], with_commit: bool = False) -> None:
    session.add_all(entities)
    flush_or_commit(with_commit)


def delete(entity: Any, with_commit: bool = False) -> None:
    session.delete(entity)
    flush_or_commit(with_commit)


def commit() -> None:
    session.commit()


def remove_and_refresh_session(
    session_token: Any, request_new: bool = False
) -> Union[Any, None]:
    check_and_roll()
    reset_ctx_token(session_token, True)
    if request_new:
        return get_ctx_token()


def flush() -> None:
    session.flush()


def flush_or_commit(commit: bool = False) -> None:
    if commit is None:
        return

    if commit:
        session.commit()
    else:
        session.flush()


def execute(sql: Any, *args) -> Any:
    return session.execute(sql, *args)


def execute_all(sql: str) -> List[Any]:
    return session.execute(sql).all()


def execute_first(sql: str) -> Any:
    return session.execute(sql).first()


def execute_distinct_count(count_sql: str) -> int:
    return session.execute(count_sql).first().distinct_count


def set_seed(seed: float = 0) -> None:
    execute(f"SELECT setseed({seed});")


def get_bind() -> Any:
    return session.get_bind()


def get_dialect() -> Any:
    return engine.dialect


def rollback() -> None:
    session.rollback()


def remove_session() -> None:
    session.remove()


def expunge(item: Any) -> None:
    session.expunge(item)


def make_transient(item: Any) -> None:
    make_transient_original(item)


def generate_UUID_sql_string() -> str:
    return "uuid_in(md5(random()::TEXT || clock_timestamp()::TEXT)::CSTRING)"


def test_database_connection() -> Dict[str, Any]:
    result = {}
    try:
        session.execute("SELECT 1")
        result["success"] = True
        result["error"] = None
    except Exception as e:
        result["success"] = False
        result["error"] = str(e.__class__.__name__)
    return result


def refresh(obj: Any) -> Any:
    session.refresh(obj)
    return obj


INDENT = "    "


def construct_select_columns(
    table: str,
    table_schema: Optional[str] = None,
    prefix: Optional[str] = None,
    exclude_columns: Optional[Union[str, List[str]]] = None,
    include_columns: Optional[Union[str, List[str]]] = None,
    indent: int = 1,
) -> str:
    table_enum: Tablenames = try_parse_enum_value(table, Tablenames)

    if table_schema is None:
        table_schema = "public"

    if not prefix:
        prefix = ""
    else:
        prefix += "."

    column_exclusion = ""
    column_inclusion = ""
    if exclude_columns or include_columns:
        if exclude_columns:
            if isinstance(exclude_columns, str):
                column_exclusion = f"AND c.column_name != '{exclude_columns}'"
            else:
                column_exclusion = (
                    "AND c.column_name NOT IN ('" + "','".join(exclude_columns) + "')"
                )
        if include_columns:
            if isinstance(include_columns, str):
                column_inclusion = f"AND c.column_name = '{include_columns}'"
            else:
                column_inclusion = (
                    "AND c.column_name IN ('" + "','".join(include_columns) + "')"
                )
    else:
        return prefix + "*"

    query = f"""
    SELECT column_name
    FROM information_schema.columns As c
    WHERE table_name = '{table_enum.value}'
    AND c.table_schema = '{table_schema}'
    {column_exclusion}
    {column_inclusion}
    ORDER BY ordinal_position
    """

    columns = [prefix + r[0] for r in execute_all(query)]
    join_on_me = ",\n"
    join_on_me += INDENT * indent
    return join_on_me.join(columns)


# aimed to create a simple SELECT x,y,z FROM table WHERE condition
def simple_selection_builder(
    table: str,
    table_schema: Optional[str] = None,
    exclude_columns: Optional[Union[str, List[str]]] = None,
    include_columns: Optional[Union[str, List[str]]] = None,
    where_condition: Optional[str] = None,
    order_by: Optional[str] = None,
) -> str:
    table_enum: Tablenames = try_parse_enum_value(table, Tablenames)

    if table_schema is None:
        table_schema = "public"
    where = ""
    if where_condition:
        where = f"WHERE {where_condition}"
    order_by_s = ""
    if order_by:
        order_by_s = f"ORDER BY {order_by}"
    return f"""
    SELECT 
    {construct_select_columns(table, table_schema,None, exclude_columns, include_columns)}
    FROM {table_schema}.{table_enum.value}
    {where}
    {order_by_s}
    """
