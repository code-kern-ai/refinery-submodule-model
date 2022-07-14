from typing import List, Any

from ..session import session
from ..models import UserSessions
from ..business_objects import general


def get(project_id: str, session_id: str) -> UserSessions:
    return (
        session.query(UserSessions)
        .filter(
            UserSessions.project_id == project_id,
            UserSessions.id == session_id,
        )
        .first()
    )


def create(user_session_data: Any, with_commit: bool = False) -> UserSessions:
    user_session: UserSessions = UserSessions(
        project_id=user_session_data.project_id,
        id_sql_statement=user_session_data.id_sql_statement,
        count_sql_statement=user_session_data.count_sql_statement,
        last_count=user_session_data.last_count,
        created_by=user_session_data.created_by,
        random_seed=user_session_data.random_seed,
    )
    general.add(user_session, with_commit)
    return user_session


def set_record_ids(
    project_id: str,
    user_session_id: str,
    record_ids: List[str],
    with_commit: bool = False,
) -> None:
    user_session = get(project_id, user_session_id)
    user_session.session_record_ids = record_ids
    general.flush_or_commit(with_commit)


def delete(project_id: str, user_id: str, with_commit: bool = False) -> None:
    session.query(UserSessions).filter(
        UserSessions.project_id == project_id,
        UserSessions.created_by == user_id,
        UserSessions.temp_session == True,
    ).delete()
    general.flush_or_commit(with_commit)
