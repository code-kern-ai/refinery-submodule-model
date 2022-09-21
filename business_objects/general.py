import uuid
from typing import Any, List, Optional, Union
from sqlalchemy.orm.session import make_transient as make_transient_original
from ..session import session
from ..session import request_id_ctx_var
from ..session import check_session_and_rollback as check_and_roll


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


def execute(sql: str) -> Any:
    return session.execute(sql)


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
