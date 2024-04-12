from typing import Dict, List, Optional, Tuple, Any, Union

from datetime import datetime

from ..cognition_objects import message
from ..business_objects import general
from ..session import session
from ..models import CognitionConversation
from ..util import prevent_sql_injection


def get(project_id: str, conversation_id: str) -> CognitionConversation:
    return (
        session.query(CognitionConversation)
        .filter(
            CognitionConversation.project_id == project_id,
            CognitionConversation.id == conversation_id,
        )
        .first()
    )


def get_count(project_id: str) -> int:
    return (
        session.query(CognitionConversation)
        .filter(CognitionConversation.project_id == project_id)
        .count()
    )


def get_overview_list(
    project_id: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    conversation_id: Optional[str] = None,
    order_desc: bool = True,
    as_query: bool = False,
) -> Union[str, List[Any]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    pagination_add = ""
    basic_where_add = ""
    order_key = "DESC" if order_desc else "ASC"

    if limit is not None:
        limit = prevent_sql_injection(limit, isinstance(limit, int))
        pagination_add += f"LIMIT {limit}"
    if offset is not None:
        offset = prevent_sql_injection(offset, isinstance(offset, int))
        pagination_add += f" OFFSET {offset}"
    if conversation_id is not None:
        conversation_id = prevent_sql_injection(
            conversation_id, isinstance(conversation_id, str)
        )
        basic_where_add += f" AND c.id = '{conversation_id}'"

    query = f"""
    SELECT x.id::TEXT conversation_id, array_agg(message_data ORDER BY z.created_at asc) message_data
    FROM (
        SELECT id, project_id, created_at, error IS NOT NULL has_error
        FROM cognition.conversation c
        WHERE c.project_id = '{project_id}' {basic_where_add}
        ORDER BY c.created_at {order_key}
        {pagination_add}
    ) x
    INNER JOIN (
        SELECT jsonb_build_object('message_id',m.id, 'question', m.question, 'has_error',y.has_error, 'time_elapsed',zx.time_elapsed) message_data, m.created_at, m.conversation_id, m.project_id
        FROM cognition.message m
        LEFT JOIN LATERAL (
            -- most recent log for message
            SELECT pl.has_error
            FROM cognition.pipeline_logs pl
            WHERE m.project_id = pl.project_id
                AND m.id = pl.message_id
            ORDER BY pl.created_at DESC
            LIMIT 1
        ) y
            ON TRUE
        INNER JOIN (
            SELECT pl.project_id, pl.message_id , SUM(pl.time_elapsed)::NUMERIC(10,5) time_elapsed
            FROM cognition.pipeline_logs pl
            GROUP BY pl.project_id, pl.message_id
        ) zx
            ON m.project_id = zx.project_id AND m.id = zx.message_id
    ) z
        ON x.project_id = z.project_id AND x.id = z.conversation_id
    GROUP BY x.id
    ORDER BY MIN(x.created_at) {order_key} """
    if as_query:
        return query
    return general.execute_all(query)


def get_all_paginated_by_project_id(
    project_id: str, page: int, limit: int, order_asc: bool = True, user_id: str = None
) -> Tuple[int, int, List[CognitionConversation]]:
    total_count_query = session.query(CognitionConversation.id).filter(
        CognitionConversation.project_id == project_id
    )
    if user_id is not None:
        total_count_query = total_count_query.filter(
            CognitionConversation.created_by == user_id
        )
    total_count = total_count_query.count()

    if total_count == 0:
        num_pages = 0
    else:
        num_pages = int(total_count / limit)
        if total_count % limit > 0:
            num_pages += 1

    if page == -1:
        page = num_pages

    if page > 0:
        query = session.query(CognitionConversation).filter(
            CognitionConversation.project_id == project_id
        )
        if user_id is not None:
            query = query.filter(CognitionConversation.created_by == user_id)
        if order_asc:
            query = query.order_by(CognitionConversation.created_at.asc())
        else:
            query = query.order_by(CognitionConversation.created_at.desc())
        paginated_result = query.limit(limit).offset((page - 1) * limit).all()
    else:
        paginated_result = []
    return total_count, num_pages, paginated_result


def create(
    project_id: str,
    user_id: str,
    has_tmp_files: bool = False,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionConversation:
    conversation: CognitionConversation = CognitionConversation(
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        has_tmp_files=has_tmp_files,
        scope_dict={},
    )
    general.add(conversation, with_commit)
    return conversation


# TODO: replace usage with create from message.py and delete this function
def add_message(
    project_id: str,
    conversation_id: str,
    user_id: str,
    question: str,
    with_commit: bool = True,
) -> CognitionConversation:
    message_entity = message.create(
        conversation_id=conversation_id,
        project_id=project_id,
        user_id=user_id,
        question=question,
        with_commit=with_commit,
    )

    return message_entity


def update(
    project_id: str,
    conversation_id: str,
    scope_dict: Optional[Dict[str, Any]] = None,
    header: Optional[str] = None,
    error: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionConversation:
    conversation_entity = get(project_id, conversation_id)
    if scope_dict is not None:
        conversation_entity.scope_dict = scope_dict
    if header is not None:
        conversation_entity.header = header
    if error is not None:
        conversation_entity.error = error
    general.flush_or_commit(with_commit)
    return conversation_entity


def clear_error(
    project_id: str,
    conversation_id: str,
    with_commit: bool = True,
) -> CognitionConversation:
    conversation_entity = get(project_id, conversation_id)
    conversation_entity.error = None
    general.flush_or_commit(with_commit)
    return conversation_entity


# TODO: replace usage with update from message.py and delete this function
def update_message(
    project_id: str,
    conversation_id: str,
    message_id: str,
    answer: Optional[str] = None,
    feedback_value: Optional[str] = None,
    feedback_category: Optional[str] = None,
    feedback_message: Optional[str] = None,
    strategy_id: Optional[str] = None,
    scope_dict_diff_previous_conversation: Optional[Dict[str, Any]] = None,
    with_commit: bool = True,
) -> CognitionConversation:
    message_entity = message.get(project_id, message_id)
    if strategy_id is not None:
        message_entity.strategy_id = strategy_id
    if answer is not None:
        message_entity.answer = answer
    if feedback_value is not None:
        message_entity.feedback_value = feedback_value
    if feedback_category is not None:
        message_entity.feedback_category = feedback_category
    if feedback_message is not None:
        message_entity.feedback_message = feedback_message
    if scope_dict_diff_previous_conversation is not None:
        message_entity.scope_dict_diff_previous_conversation = (
            scope_dict_diff_previous_conversation
        )
    general.flush_or_commit(with_commit)
    conversation_entity = get(project_id, conversation_id)
    return conversation_entity


def delete(project_id: str, conversation_id: str, with_commit: bool = True) -> None:
    session.query(CognitionConversation).filter(
        CognitionConversation.project_id == project_id,
        CognitionConversation.id == conversation_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_many(
    project_id: str, conversation_ids: List[str], with_commit: bool = True
) -> None:
    session.query(CognitionConversation).filter(
        CognitionConversation.project_id == project_id,
        CognitionConversation.id.in_(conversation_ids),
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)
