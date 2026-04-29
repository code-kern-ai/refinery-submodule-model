from typing import Dict, List, Optional, Tuple, Any, Union

from datetime import datetime, timedelta

from ..cognition_objects import message
from ..business_objects import general
from ..session import session
from ..models import (
    CognitionConversation,
    CognitionMessage,
    CognitionConversationTagAssociation,
    CognitionPipelineLogs,
)
from ..util import prevent_sql_injection
from sqlalchemy.sql.expression import Subquery
from sqlalchemy import and_, case, func, nullslast, or_
from sqlalchemy.sql.expression import cast
from sqlalchemy import String as sqlalchemy_string
from sqlalchemy import select


_CONVERSATION_LIST_SORT_COLUMNS = {
    "created_at": CognitionConversation.created_at,
    "header": CognitionConversation.header,
    "archived": CognitionConversation.archived,
    "has_tmp_files": CognitionConversation.has_tmp_files,
    "error": CognitionConversation.error,
    "incognito_mode": CognitionConversation.incognito_mode,
    "created_by": CognitionConversation.created_by,
}

_EXTRA_CONVERSATION_SORT_KEYS = frozenset(
    {
        "initial_message",
        "initial_strategy",
        "num_queries",
        "num_answers",
        "num_success",
        "num_error",
    }
)


def __first_message_text_subquery(project_id: str):
    ranked = (
        session.query(
            CognitionMessage.conversation_id.label("conversation_id"),
            CognitionMessage.question.label("initial_question"),
            func.row_number()
            .over(
                partition_by=CognitionMessage.conversation_id,
                order_by=(
                    CognitionMessage.created_at.asc(),
                    CognitionMessage.id.asc(),
                ),
            )
            .label("msg_rn"),
        )
        .filter(CognitionMessage.project_id == project_id)
    ).subquery()
    return (
        session.query(
            ranked.c.conversation_id,
            ranked.c.initial_question,
        )
        .filter(ranked.c.msg_rn == 1)
    ).subquery()


def __first_message_strategy_subquery(project_id: str):
    ranked = (
        session.query(
            CognitionMessage.conversation_id.label("conversation_id"),
            CognitionMessage.strategy_id.label("initial_strategy_id"),
            func.row_number()
            .over(
                partition_by=CognitionMessage.conversation_id,
                order_by=(
                    CognitionMessage.created_at.asc(),
                    CognitionMessage.id.asc(),
                ),
            )
            .label("msg_rn"),
        )
        .filter(CognitionMessage.project_id == project_id)
    ).subquery()
    return (
        session.query(
            ranked.c.conversation_id,
            ranked.c.initial_strategy_id,
        )
        .filter(ranked.c.msg_rn == 1)
    ).subquery()


def __messages_with_log_errors_subquery(project_id: str):
    """Per message: SUM(has_error) > 0, matching table_view / get_error_and_time_elapsed."""
    inner = (
        session.query(
            CognitionMessage.id.label("message_id"),
            CognitionMessage.conversation_id.label("conversation_id"),
        )
        .select_from(CognitionMessage)
        .join(
            CognitionPipelineLogs,
            and_(
                CognitionMessage.project_id == CognitionPipelineLogs.project_id,
                CognitionMessage.id == CognitionPipelineLogs.message_id,
            ),
        )
        .filter(CognitionMessage.project_id == project_id)
        .group_by(CognitionMessage.id, CognitionMessage.conversation_id)
        .having(
            func.sum(case((CognitionPipelineLogs.has_error.is_(True), 1), else_=0)) > 0
        )
    ).subquery()
    return (
        session.query(
            inner.c.conversation_id,
            func.count().label("num_error"),
        )
        .group_by(inner.c.conversation_id)
    ).subquery()


def __conversation_message_stats_subquery(project_id: str):
    """Aggregates used for conversations table sorting (aligned with cognition-ui table prep)."""
    err_sq = __messages_with_log_errors_subquery(project_id)
    msg_agg = (
        session.query(
            CognitionMessage.conversation_id.label("conversation_id"),
            func.count(CognitionMessage.id).label("num_queries"),
            func.sum(
                case(
                    (
                        and_(
                            CognitionMessage.answer.isnot(None),
                            CognitionMessage.answer != "",
                        ),
                        1,
                    ),
                    else_=0,
                )
            ).label("num_answers"),
        )
        .filter(CognitionMessage.project_id == project_id)
        .group_by(CognitionMessage.conversation_id)
    ).subquery()
    return (
        session.query(
            msg_agg.c.conversation_id,
            msg_agg.c.num_queries,
            msg_agg.c.num_answers,
            func.coalesce(err_sq.c.num_error, 0).label("num_error"),
            (
                msg_agg.c.num_queries - func.coalesce(err_sq.c.num_error, 0)
            ).label("num_success"),
        )
        .select_from(msg_agg)
        .outerjoin(err_sq, msg_agg.c.conversation_id == err_sq.c.conversation_id)
    ).subquery()


def __normalize_conversation_sort_by(sort_by: Optional[str]) -> str:
    if sort_by is None or not str(sort_by).strip():
        return "created_at"
    s = str(sort_by).strip()
    key_map = {
        "createdAt": "created_at",
        "hasTmpFiles": "has_tmp_files",
        "incognitoMode": "incognito_mode",
        "createdBy": "created_by",
        "initialMessage": "initial_message",
        "initialStrategy": "initial_strategy",
    }
    if s in key_map:
        return key_map[s]
    normalized = s.lower().replace("-", "_")
    if normalized in _CONVERSATION_LIST_SORT_COLUMNS:
        return normalized
    if normalized in _EXTRA_CONVERSATION_SORT_KEYS:
        return normalized
    return "created_at"


def __resolve_conversation_list_sort_key_and_asc(
    sort_by: Optional[str],
    sort_direction: Optional[str],
) -> Tuple[str, bool]:
    sort_key = __normalize_conversation_sort_by(sort_by)
    has_explicit_direction = (
        sort_direction is not None and str(sort_direction).strip() != ""
    )
    if has_explicit_direction:
        asc = str(sort_direction).strip().upper() == "ASC"
    else:
        asc = False
    return sort_key, asc


def get(project_id: str, conversation_id: str) -> CognitionConversation:
    return (
        session.query(CognitionConversation)
        .filter(
            CognitionConversation.project_id == project_id,
            CognitionConversation.id == conversation_id,
        )
        .first()
    )


def get_by_id(conversation_id: str) -> CognitionConversation:
    return (
        session.query(CognitionConversation)
        .filter(CognitionConversation.id == conversation_id)
        .first()
    )


def exists(project_id: str, conversation_id: str) -> bool:
    return (
        session.query(CognitionConversation)
        .filter(
            CognitionConversation.project_id == project_id,
            CognitionConversation.id == conversation_id,
        )
        .first()
        is not None
    )


def get_conversations_to_clean_up() -> List[Tuple[str, str, str]]:
    query = """
    SELECT cc.id, cc.project_id, o.id organization_id
    FROM cognition.conversation cc
    INNER JOIN cognition.project cp
        ON cc.project_id = cp.id
    INNER JOIN (
        SELECT o.*, NOW() - INTERVAL '1 DAY' * conversation_lifespan_days conversation_delete_by
        FROM PUBLIC.organization o
    ) o
        ON cp.organization_id = o.id AND cc.created_at <= o.conversation_delete_by"""
    return general.execute_all(query)


def get_conversation_files_to_clean_up() -> List[Tuple[str, str, str]]:
    query = """
    SELECT cc.id, cc.project_id, o.id organization_id
    FROM cognition.conversation cc
    INNER JOIN cognition.project cp
        ON cc.project_id = cp.id
    INNER JOIN (
        SELECT o.*, NOW() - INTERVAL '1 DAY' * file_lifespan_days file_delete_by
        FROM PUBLIC.organization o
    ) o
        ON cp.organization_id = o.id AND cc.created_at <= o.file_delete_by
    WHERE NOT cc.archived AND cc.has_tmp_files """
    return general.execute_all(query)


def get_scoped(project_id: str, conversation_id: str, user_id) -> CognitionConversation:
    return (
        session.query(CognitionConversation)
        .filter(
            CognitionConversation.project_id == project_id,
            CognitionConversation.id == conversation_id,
            CognitionConversation.created_by == user_id,
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
        WHERE c.project_id = '{project_id}' {basic_where_add} AND c.incognito_mode = FALSE
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
    project_id: str,
    page: int,
    limit: int,
    user_id: Optional[str] = None,
    filter_dict: Optional[Dict[str, Any]] = None,
    filter_incognito: bool = False,
    sort_by: Optional[str] = None,
    sort_direction: Optional[str] = None,
) -> Tuple[int, int, List[CognitionConversation]]:
    total_count_query = session.query(CognitionConversation.id).filter(
        CognitionConversation.project_id == project_id
    )
    if filter_incognito:
        total_count_query = total_count_query.filter(
            CognitionConversation.incognito_mode == False
        )
    subquery = None
    if filter_dict is not None:
        subquery = __get_conversation_ids_by_filter(project_id, **filter_dict)
        total_count_query = total_count_query.filter(
            CognitionConversation.id.in_(subquery)
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
        if filter_incognito:
            query = query.filter(CognitionConversation.incognito_mode == False)
        if user_id is not None:
            query = query.filter(CognitionConversation.created_by == user_id)
        if subquery is not None:
            query = query.filter(CognitionConversation.id.in_(subquery))
        sort_key, sort_asc = __resolve_conversation_list_sort_key_and_asc(
            sort_by, sort_direction
        )
        if sort_key == "initial_message":
            first_msg_sq = __first_message_text_subquery(project_id)
            query = query.outerjoin(
                first_msg_sq,
                CognitionConversation.id == first_msg_sq.c.conversation_id,
            )
            order_col = func.lower(first_msg_sq.c.initial_question)
            if sort_asc:
                query = query.order_by(
                    nullslast(order_col.asc()),
                    CognitionConversation.id.asc(),
                )
            else:
                query = query.order_by(
                    nullslast(order_col.desc()),
                    CognitionConversation.id.desc(),
                )
        elif sort_key == "initial_strategy":
            first_strat_sq = __first_message_strategy_subquery(project_id)
            query = query.outerjoin(
                first_strat_sq,
                CognitionConversation.id == first_strat_sq.c.conversation_id,
            )
            order_col = first_strat_sq.c.initial_strategy_id
            if sort_asc:
                query = query.order_by(
                    nullslast(order_col.asc()),
                    CognitionConversation.id.asc(),
                )
            else:
                query = query.order_by(
                    nullslast(order_col.desc()),
                    CognitionConversation.id.desc(),
                )
        elif sort_key in (
            "num_queries",
            "num_answers",
            "num_success",
            "num_error",
        ):
            stats_sq = __conversation_message_stats_subquery(project_id)
            query = query.outerjoin(
                stats_sq,
                CognitionConversation.id == stats_sq.c.conversation_id,
            )
            order_map = {
                "num_queries": func.coalesce(stats_sq.c.num_queries, 0),
                "num_answers": func.coalesce(stats_sq.c.num_answers, 0),
                "num_success": func.coalesce(stats_sq.c.num_success, 0),
                "num_error": func.coalesce(stats_sq.c.num_error, 0),
            }
            order_col = order_map[sort_key]
            if sort_asc:
                query = query.order_by(
                    order_col.asc(),
                    CognitionConversation.id.asc(),
                )
            else:
                query = query.order_by(
                    order_col.desc(),
                    CognitionConversation.id.desc(),
                )
        else:
            sort_col = _CONVERSATION_LIST_SORT_COLUMNS[sort_key]
            if sort_asc:
                query = query.order_by(
                    sort_col.asc(),
                    CognitionConversation.id.asc(),
                )
            else:
                query = query.order_by(
                    sort_col.desc(),
                    CognitionConversation.id.desc(),
                )
        paginated_result = query.limit(limit).offset((page - 1) * limit).all()
    else:
        paginated_result = []
    return total_count, num_pages, paginated_result


def get_missing_tagged_conversations(
    project_id: str, user_id: str, tag_id: str, not_needed_conversations: List[str]
) -> List[CognitionConversation]:
    missing = (
        session.query(CognitionConversation)
        .join(
            CognitionConversationTagAssociation,
            (
                CognitionConversationTagAssociation.conversation_id
                == CognitionConversation.id
            ),
        )
        .filter(
            CognitionConversationTagAssociation.tag_id == tag_id,
            CognitionConversation.id.notin_(not_needed_conversations),
            CognitionConversation.project_id == project_id,
            CognitionConversation.created_by == user_id,
        )
    ).all()
    return missing


def __get_conversation_ids_by_filter(
    project_id: str,
    user_id: Optional[str] = None,
    has_error: Optional[bool] = None,
    has_tmp_files: Optional[bool] = None,
    tmp_file_name: Optional[str] = None,
    question_or_answer: Optional[str] = None,
    fact_contains: Optional[str] = None,
    feedback_value: Optional[str] = None,
    feedback_message_contains: Optional[str] = None,
    created_at_from: Optional[str] = None,
    created_at_to: Optional[str] = None,
) -> Subquery:
    query = select(CognitionConversation.id).filter(
        CognitionConversation.project_id == project_id
    )
    if created_at_from is not None:
        query = query.filter(CognitionConversation.created_at >= created_at_from)
    if created_at_to is not None:
        query = query.filter(CognitionConversation.created_at <= created_at_to)
    if user_id is not None:
        query = query.filter(CognitionConversation.created_by == user_id)
    if has_error is not None:
        if has_error:
            query = query.filter(CognitionConversation.error.isnot(None))
        else:
            query = query.filter(CognitionConversation.error.is_(None))
    if has_tmp_files is not None:
        query = query.filter(CognitionConversation.has_tmp_files == has_tmp_files)
    if tmp_file_name is not None:
        tmp_file_name = "%" + tmp_file_name + "%"
        query = query.filter(
            CognitionConversation.scope_dict.op("->>")("parsed_documents").ilike(
                tmp_file_name
            )
        )
    if (
        question_or_answer is not None
        or fact_contains is not None
        or feedback_value is not None
        or feedback_message_contains is not None
    ):
        query = query.join(
            CognitionMessage,
            (CognitionMessage.project_id == CognitionConversation.project_id)
            & (CognitionMessage.conversation_id == CognitionConversation.id),
        )
    if question_or_answer is not None:
        question_or_answer = "%" + question_or_answer + "%"
        query = query.filter(
            or_(
                CognitionMessage.question.ilike(question_or_answer),
                CognitionMessage.answer.ilike(question_or_answer),
            )
        )
    if fact_contains is not None:
        fact_contains = "%" + fact_contains + "%"
        query = query.filter(
            cast(CognitionMessage.facts, sqlalchemy_string).ilike(fact_contains)
        )
    if feedback_value is not None:
        query = query.filter(CognitionMessage.feedback_value == feedback_value)
    if feedback_message_contains is not None:
        feedback_message_contains = "%" + feedback_message_contains + "%"
        query = query.filter(
            CognitionMessage.feedback_message.ilike(feedback_message_contains)
        )
    return query


def has_error(project_id: str, conversation_id: str) -> bool:
    conversation_item = get(project_id, conversation_id)
    if conversation_item is None:
        return False
    if conversation_item.error is not None:
        return True
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    conversation_id = prevent_sql_injection(
        conversation_id, isinstance(conversation_id, str)
    )
    query = f"""
    SELECT DISTINCT pl.has_error
    FROM cognition.conversation C
    INNER JOIN cognition.message M
        ON c.project_id = m.project_id AND c.id = m.conversation_id
    INNER JOIN cognition.pipeline_logs pl
        ON m.project_id = pl.project_id AND m.id = pl.message_id
    WHERE c.id = '{conversation_id}' AND c.project_id = '{project_id}'
    ORDER BY 1 DESC -- true first
    LIMIT 1 """

    result = general.execute_first(query)
    if result and result[0] == True:
        return True
    return False


def create_and_get_id(
    project_id: str,
    user_id: str,
    has_tmp_files: bool = False,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionConversation:
    created_conversation = create(
        project_id=project_id,
        user_id=user_id,
        has_tmp_files=has_tmp_files,
        with_commit=with_commit,
        created_at=created_at,
    )
    return str(created_conversation.id)


def create(
    project_id: str,
    user_id: str,
    has_tmp_files: bool = False,
    is_incognito: bool = False,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionConversation:
    conversation: CognitionConversation = CognitionConversation(
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        has_tmp_files=has_tmp_files,
        scope_dict={},
        incognito_mode=is_incognito,
    )
    general.add(conversation, with_commit)
    return conversation


def update(
    project_id: str,
    conversation_id: str,
    scope_dict: Optional[Dict[str, Any]] = None,
    header: Optional[str] = None,
    error: Optional[str] = None,
    incognito_mode: Optional[bool] = None,
    with_commit: bool = True,
) -> CognitionConversation:
    conversation_entity = get(project_id, conversation_id)
    if scope_dict is not None:
        conversation_entity.scope_dict = scope_dict
    if header is not None:
        conversation_entity.header = header
    if error is not None:
        conversation_entity.error = error
    if incognito_mode is not None:
        conversation_entity.incognito_mode = incognito_mode
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
    scope_dict_diff_new: Optional[Dict[str, Any]] = None,
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
    if scope_dict_diff_new is not None:
        message_entity.scope_dict_diff_new = scope_dict_diff_new
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


def delete_incognito_conversations_older_than_24_hours() -> None:
    time_to_delete = datetime.now() - timedelta(hours=24)
    session.query(CognitionConversation).filter(
        CognitionConversation.incognito_mode == True,
        CognitionConversation.created_at <= time_to_delete,
    ).delete(synchronize_session=False)
    general.flush_or_commit(True)
