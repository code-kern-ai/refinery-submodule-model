import uuid
from typing import Any, Dict, List, Optional, Union
from sqlalchemy.orm.session import make_transient as make_transient_original
from ..session import session, engine
from ..session import request_id_ctx_var
from ..session import check_session_and_rollback as check_and_roll
from ..enums import Tablenames, try_parse_enum_value


def get_ctx_token() -> Any:
    return request_id_ctx_var.set(str(uuid.uuid4()))


def reset_ctx_token(ctx_token: Any, remove_db: Optional[bool] = False) -> None:
    if remove_db:
        session.remove()
    request_id_ctx_var.reset(ctx_token)


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
