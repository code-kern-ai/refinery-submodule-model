from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime

from submodules.model.enums import MessageType
from submodules.model.business_objects import cross_selling as cross_selling_bo
from ..business_objects import general
from ..session import session
from ..models import CognitionMessage
from ..util import prevent_sql_injection, to_snake_case
from .pipeline_version import get_current_version
from sqlalchemy.orm.attributes import flag_modified


DEFAULT_TIME_ELAPSED = {
    "time_elapsed": 0,
    "has_error": True,
    "answer": "",
}


def get_all_by_conversation_id(
    project_id: str, conversation_id: str
) -> List[CognitionMessage]:
    return (
        session.query(CognitionMessage)
        .filter(
            CognitionMessage.project_id == project_id,
            CognitionMessage.conversation_id == conversation_id,
        )
        .order_by(CognitionMessage.created_at.asc())
        .all()
    )


def get_all_by_conversation_ids(
    project_id: str, conversation_ids: List[str]
) -> Dict[str, List[CognitionMessage]]:

    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    conversation_ids = [
        prevent_sql_injection(conversation_id, isinstance(conversation_id, str))
        for conversation_id in conversation_ids
    ]

    if not conversation_ids:
        return {}

    conversation_where = (
        " AND conversation_id IN ('" + "','".join(conversation_ids) + "')"
    )
    query = f"""
    SELECT jsonb_object_agg(conversation_id, messages)
    FROM (
        SELECT m.conversation_id, array_agg(row_to_json(m) ORDER BY created_at ASC) AS messages
        FROM cognition.message m
        WHERE project_id = '{project_id}'{conversation_where}
        GROUP BY conversation_id
    ) x
    """

    message_info = general.execute_first(query)
    if message_info and message_info[0]:
        return message_info[0]
    return {}


def get_last_by_conversation_id(
    project_id: str, conversation_id: str
) -> CognitionMessage:
    return (
        session.query(CognitionMessage)
        .filter(
            CognitionMessage.project_id == project_id,
            CognitionMessage.conversation_id == conversation_id,
        )
        .order_by(CognitionMessage.created_at.desc())
        .first()
    )


def get_last_n_by_conversation_id(
    project_id: str, conversation_id: str, n: int
) -> List[CognitionMessage]:
    return (
        session.query(CognitionMessage)
        .filter(
            CognitionMessage.project_id == project_id,
            CognitionMessage.conversation_id == conversation_id,
        )
        .order_by(CognitionMessage.created_at.desc())
        .limit(n)
        .all()
    )


def get_message_ids_with_version_id(project_id: str, version_id: str) -> List[str]:
    return [
        str(e.id)
        for e in session.query(CognitionMessage)
        .filter(
            CognitionMessage.project_id == project_id,
            CognitionMessage.version_id == version_id,
        )
        .all()
    ]


def get_scope_changes_before_message(
    project_id: str, message_id: str
) -> List[List[Dict[str, Any]]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    message_id = prevent_sql_injection(message_id, isinstance(message_id, str))

    query = f"""
    SELECT COALESCE(array_agg(m2.scope_dict_diff_new ORDER BY m2.created_at asc),ARRAY[]::JSON[]) changes
    FROM cognition.message m
    INNER JOIN cognition.message m2
        ON m.project_id = m2.project_id AND m.conversation_id = m2.conversation_id 
        AND m.id != m2.id AND m2.created_at < m.created_at
    WHERE m.project_id = '{project_id}' AND m.id = '{message_id}'
"""

    result = general.execute_first(query)
    if result:
        return result[0]
    return []


def get_message_short_for_conversation_for_pipeline(
    project_id: str, conversation_id: str
) -> List[Dict[str, str]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    conversation_id = prevent_sql_injection(
        conversation_id, isinstance(conversation_id, str)
    )
    query = f"""
    SELECT jsonb_object_agg(message_id,json_build_object('time_elapsed',time_elapsed,'has_error',CASE WHEN has_error = 1 THEN TRUE ELSE FALSE END, 'strategy_id', strategy_id, 'answer', answer,'version_id',version_id))
    FROM (
        SELECT 
            pl.message_id,
            MAX(m.strategy_id::TEXT) strategy_id, 
            MAX(m.answer) answer, 
            sum(pl.time_elapsed)time_elapsed, 
            MAX(CASE WHEN pl.has_error THEN 1 ELSE 0 END) has_error,
            max(version_id::TEXT) version_id
        FROM cognition.message m
        INNER JOIN cognition.pipeline_logs pl
            ON m.project_id = pl.project_id AND m.id = pl.message_id
        WHERE pl.project_id = '{project_id}' AND m.conversation_id = '{conversation_id}'
        GROUP BY pl.message_id )x """

    time_elapsed = general.execute_first(query)
    if time_elapsed and time_elapsed[0]:
        time_elapsed = time_elapsed[0]
    else:
        time_elapsed = {}

    return [
        {
            "id": str(e.id),
            "question": e.question,
            "created_at": str(e.created_at),
            **time_elapsed.get(str(e.id), DEFAULT_TIME_ELAPSED),
        }
        for e in (
            session.query(CognitionMessage)
            .filter(
                CognitionMessage.project_id == project_id,
                CognitionMessage.conversation_id == conversation_id,
            )
            .order_by(CognitionMessage.created_at.asc())
            .all()
        )
    ]


def get(project_id: str, message_id: str) -> CognitionMessage:
    return (
        session.query(CognitionMessage)
        .filter(
            CognitionMessage.project_id == project_id,
            CognitionMessage.id == message_id,
        )
        .first()
    )


def get_by_strategy_id(project_id: str, strategy_id: str) -> CognitionMessage:
    return (
        session.query(CognitionMessage)
        .filter(
            CognitionMessage.project_id == project_id,
            CognitionMessage.strategy_id == strategy_id,
        )
        .first()
    )


def _coerce_feedback_overview_query_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        if not val:
            return None
        for item in val:
            coerced = _coerce_feedback_overview_query_str(item)
            if coerced is not None:
                return coerced
        return None
    s = val.strip() if isinstance(val, str) else str(val).strip()
    if not s:
        return None
    low = s.lower()
    if low in ("undefined", "null", "(null)"):
        return None
    return s


def _normalize_feedback_overview_value_filter(feedback_value: Any) -> Optional[str]:
    s = _coerce_feedback_overview_query_str(feedback_value)
    if not s:
        return None
    v = s.lower()
    if v == "all":
        return None
    if v in ("positive", "negative", "neutral"):
        return v
    return None


def _feedback_overview_value_filter_invalid(feedback_value: Any) -> bool:
    s = _coerce_feedback_overview_query_str(feedback_value)
    if s is None or s.lower() == "all":
        return False
    return _normalize_feedback_overview_value_filter(feedback_value) is None


def _message_feedback_overview_where_add(
    project_id: str,
    start_date: Optional[str],
    end_date: Optional[str],
    only_with_feedback: bool,
    search: Optional[str] = None,
    feedback_value: Optional[str] = None,
    only_with_feedback_message: bool = False,
) -> Tuple[str, str]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    where_add = ""

    if only_with_feedback:
        where_add += "AND (mo.feedback_value IS NOT NULL OR y.has_error)"

    fv = _normalize_feedback_overview_value_filter(feedback_value)
    if fv == "positive":
        where_add += "AND mo.feedback_value = 'positive'"
    elif fv == "negative":
        where_add += "AND mo.feedback_value = 'negative'"
    elif fv == "neutral":
        where_add += "AND (mo.feedback_value = 'neutral' OR mo.feedback_value IS NULL)"

    if start_date and end_date:
        start_date_s = prevent_sql_injection(start_date, isinstance(start_date, str))
        end_date_s = prevent_sql_injection(end_date, isinstance(end_date, str))
        where_add += f"AND mo.created_at BETWEEN '{start_date_s}' AND '{end_date_s}'"
    elif start_date:
        start_date_s = prevent_sql_injection(start_date, isinstance(start_date, str))
        where_add += f"AND mo.created_at >= '{start_date_s}'"
    elif end_date:
        end_date_s = prevent_sql_injection(end_date, isinstance(end_date, str))
        where_add += f"AND mo.created_at <= '{end_date_s}'"

    search_c = _coerce_feedback_overview_query_str(search)
    if search_c:
        search_s = prevent_sql_injection(search_c, isinstance(search_c, str))
        where_add += f"""
        AND (
            COALESCE(mo.feedback_message, '') ILIKE '%{search_s}%'
            OR COALESCE(mo.feedback_category, '') ILIKE '%{search_s}%'
            OR COALESCE(mo.question, '') ILIKE '%{search_s}%'
            OR COALESCE(mo.answer, '') ILIKE '%{search_s}%'
            OR COALESCE(x.full_conversation_text, '') ILIKE '%{search_s}%'
            OR CAST(mo.conversation_id AS TEXT) ILIKE '%{search_s}%'
            OR CAST(mo.created_by AS TEXT) ILIKE '%{search_s}%'
        )
        """

    if only_with_feedback_message:
        where_add += (
            "AND NULLIF(BTRIM(COALESCE(mo.feedback_message, '')), '') IS NOT NULL"
        )

    return where_add, project_id


def _message_feedback_overview_from_where(project_id: str, where_add: str) -> str:
    return f"""
    FROM cognition.message mo
    INNER JOIN cognition.conversation C
        ON mo.project_id = c.project_id AND mo.conversation_id = c.id
    INNER JOIN (
        SELECT
            project_id,
            conversation_id,
            string_agg('Question ' || LPAD(rn::TEXT,3,'0') || ':\n' || question || '\n\nAnswer ' || LPAD(rn::TEXT,3,'0') || ':\n'|| answer,'\n') full_conversation_text
        FROM (
            SELECT c.project_id, mi.conversation_id, COALESCE(mi.question,'<null>')question, COALESCE(mi.answer,'<null>')answer, ROW_NUMBER() OVER(PARTITION BY c.id ORDER BY mi.created_at DESC) rn
            FROM cognition.conversation C
            INNER JOIN cognition.message mi
                ON c.project_id = mi.project_id AND c.id = mi.conversation_id
            WHERE C.project_id = '{project_id}' AND C.incognito_mode = FALSE
        ) x
        GROUP BY project_id, conversation_id
    ) x
        ON c.project_id = x.project_id AND c.id = x.conversation_id
    LEFT JOIN LATERAL(
        SELECT pl.has_error, pl.content
        FROM cognition.pipeline_logs pl
        WHERE pl.project_id = mo.project_id
            AND pl.message_id = mo.id
            AND pl.has_error
        ORDER BY pl.created_at DESC
        LIMIT 1
    )y ON TRUE
    WHERE mo.project_id = '{project_id}'
    {where_add}
    """


_FV_OR_ERR_EXPR = "COALESCE(mo.feedback_value, CASE WHEN y.has_error THEN 'ERROR_IN_NEWEST_LOG' ELSE NULL END)"
_FEEDBACK_OVERVIEW_SORT_SQL: Dict[str, str] = {
    "message_created": "LOWER(COALESCE(mo.feedback_message, ''))",
    "created_at": "mo.created_at",
    "feedback_value_or_error": _FV_OR_ERR_EXPR,
    "feedback_value": _FV_OR_ERR_EXPR,
    "feedback_message": "LOWER(COALESCE(mo.feedback_message, ''))",
    "feedback_category": (
        "CASE WHEN mo.feedback_value='negative' THEN mo.feedback_category ELSE NULL END"
    ),
    "question": "mo.question",
    "answer": "mo.answer",
    "full_conversation_text": "x.full_conversation_text",
    "conversation_id": "mo.conversation_id",
    "user_id": "mo.created_by",
    "created_by": "mo.created_by",
}


def _message_feedback_overview_order_by_clause(
    sort_by: Optional[str], sort_direction: Optional[str]
) -> Tuple[str, str, str]:
    raw_key = (sort_by or "").strip()
    sort_key = to_snake_case(raw_key) if raw_key else "message_created"
    if sort_key not in _FEEDBACK_OVERVIEW_SORT_SQL:
        sort_key = "message_created"
    expr = _FEEDBACK_OVERVIEW_SORT_SQL[sort_key]
    direction = (sort_direction or "desc").strip().upper()
    if direction not in ("ASC", "DESC"):
        direction = "DESC"
    clause = f"ORDER BY {expr} {direction} NULLS LAST, mo.id DESC"
    return clause, sort_key, direction.lower()


def _message_feedback_overview_select_columns() -> str:
    return """
    SELECT
        COALESCE(feedback_value, CASE WHEN y.has_error THEN 'ERROR_IN_NEWEST_LOG' ELSE NULL END) feedback_value_or_error,
        feedback_message,
        CASE WHEN feedback_value='negative' THEN feedback_category ELSE NULL END feedback_category,
        REGEXP_REPLACE(question, \'[\\000-\\010]|[\\013-\\014]|[\\016-\\037]\',\'\',\'\') question,
        REGEXP_REPLACE(answer, \'[\\000-\\010]|[\\013-\\014]|[\\016-\\037]\',\'\',\'\') answer,
        REGEXP_REPLACE(x.full_conversation_text, \'[\\000-\\010]|[\\013-\\014]|[\\016-\\037]\',\'\',\'\') full_conversation_text,
        json_build_object(
            'message_id',mo.id,
            'conversation_id',mo.conversation_id,
            'user_id',mo.created_by,
            'message_created', mo.created_at,
            'newest_log_has_error', COALESCE(y.has_error,FALSE),
            'has_error_log_content', ARRAY_TO_STRING( y.content,'\n')
        )::TEXT message_data
    """


def get_message_feedback_overview(
    project_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    only_with_feedback: bool = True,
    as_query: bool = False,
    search: Optional[str] = None,
    feedback_value: Optional[str] = None,
    only_with_feedback_message: bool = False,
) -> Union[str, List[Dict[str, Any]]]:
    if _feedback_overview_value_filter_invalid(feedback_value):
        project_id_s = prevent_sql_injection(project_id, isinstance(project_id, str))
        if as_query:
            return (
                _message_feedback_overview_select_columns()
                + _message_feedback_overview_from_where(project_id_s, "AND 1=0")
                + "\n    ORDER BY mo.created_at DESC\n    "
            )
        return []
    where_add, project_id_s = _message_feedback_overview_where_add(
        project_id,
        start_date,
        end_date,
        only_with_feedback,
        search,
        feedback_value,
        only_with_feedback_message,
    )
    from_where = _message_feedback_overview_from_where(project_id_s, where_add)
    query = (
        _message_feedback_overview_select_columns()
        + from_where
        + """
    ORDER BY mo.created_at DESC
    """
    )
    if as_query:
        return query
    return general.execute_all(query)


def get_message_feedback_overview_paginated(
    project_id: str,
    limit: int,
    offset: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    only_with_feedback: bool = True,
    sort_by: Optional[str] = None,
    sort_direction: Optional[str] = None,
    search: Optional[str] = None,
    feedback_value: Optional[str] = None,
    only_with_feedback_message: bool = False,
) -> Dict[str, Any]:
    if _feedback_overview_value_filter_invalid(feedback_value):
        _, effective_sort_key, effective_sort_dir = (
            _message_feedback_overview_order_by_clause(sort_by, sort_direction)
        )
        return {
            "rows": [],
            "total_count": 0,
            "limit": limit,
            "offset": offset,
            "sort_by": effective_sort_key,
            "sort_direction": effective_sort_dir,
            "search": _coerce_feedback_overview_query_str(search),
            "feedback_value": None,
            "only_with_feedback_message": bool(only_with_feedback_message),
        }
    where_add, project_id_s = _message_feedback_overview_where_add(
        project_id,
        start_date,
        end_date,
        only_with_feedback,
        search,
        feedback_value,
        only_with_feedback_message,
    )
    from_where = _message_feedback_overview_from_where(project_id_s, where_add)
    order_clause, effective_sort_key, effective_sort_dir = (
        _message_feedback_overview_order_by_clause(sort_by, sort_direction)
    )
    count_query = f"""
    SELECT COUNT(*)::int AS cnt
    FROM (
        SELECT 1 AS _row
        {from_where}
    ) _sub
    """
    count_row = general.execute_first(count_query)
    total_count = int(count_row[0]) if count_row and count_row[0] is not None else 0

    data_query = (
        _message_feedback_overview_select_columns()
        + from_where
        + f"""
    {order_clause}
    LIMIT {int(limit)} OFFSET {int(offset)}
    """
    )
    rows = general.execute_all(data_query)
    return {
        "rows": rows,
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "sort_by": effective_sort_key,
        "sort_direction": effective_sort_dir,
        "search": _coerce_feedback_overview_query_str(search),
        "feedback_value": _normalize_feedback_overview_value_filter(feedback_value),
        "only_with_feedback_message": bool(only_with_feedback_message),
    }


def get_show_shield_dict_by_conversation_ids(
    project_id: str, conversation_ids: List[str]
) -> Dict[str, bool]:
    if not conversation_ids or not project_id:
        return {}
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    conversation_ids = [
        prevent_sql_injection(conversation_id, isinstance(conversation_id, str))
        for conversation_id in conversation_ids
    ]
    conversation_id_filter = "('" + "','".join(conversation_ids) + "')"
    query = f"""
    SELECT jsonb_object_agg(t.conversation_id::text, t.show_shield_icon) AS convo_shields
    FROM (
    SELECT
        c.id AS conversation_id,
        COALESCE(
        bool_and(
            COALESCE(
            (m.additional_data->'privacy_report'->>'is_private') IN ('A+','A','A-'),
            false
            )
        )
        AND
        bool_and(
            COALESCE(
            (m.additional_data->'privacy_report'->>'any_privatemode_ai')::boolean,
            false
            )
        ),
        false
        ) AS show_shield_icon
    FROM cognition.conversation c
    LEFT JOIN cognition.message m 
        ON c.project_id = m.project_id AND m.conversation_id = c.id
        WHERE c.project_id  = '{project_id}' AND c.id IN {conversation_id_filter}
    GROUP BY c.id
    ) t;"""

    show_shield_dict = general.execute_first(query)
    if show_shield_dict and show_shield_dict[0]:
        return show_shield_dict[0]
    return {}


def create(
    conversation_id: str,
    project_id: str,
    user_id: str,
    question: str,
    initiated_via: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    additional_data: Optional[Dict[str, Any]] = None,
) -> CognitionMessage:
    version_id = None
    current_version = get_current_version(project_id)
    if current_version:
        version_id = current_version.id
    message = CognitionMessage(
        project_id=project_id,
        conversation_id=conversation_id,
        created_by=user_id,
        created_at=created_at,
        question=question,
        facts=[],
        version_id=version_id,
        additional_data=additional_data or {},
        initiated_via=initiated_via,
    )

    general.add(message, with_commit)

    return message


def update(
    project_id: str,
    message_id: str,
    answer: Optional[str] = None,
    facts: Optional[List[Dict[str, Any]]] = None,
    selection_widget: Optional[List[Dict[str, Any]]] = None,
    feedback_value: Optional[str] = None,
    feedback_category: Optional[str] = None,
    feedback_message: Optional[str] = None,
    additional_data: Optional[Union[Dict[str, Any], str]] = None,
    with_commit: bool = True,
) -> CognitionMessage:
    message = get(project_id, message_id)
    if answer is not None:
        message.answer = answer
    if facts is not None:
        message.facts = facts
    if selection_widget is not None:
        message.selection_widget = selection_widget
    if feedback_value is not None:
        message.feedback_value = feedback_value
    if feedback_category is not None:
        message.feedback_category = feedback_category
    if feedback_message is not None:
        message.feedback_message = feedback_message
    if additional_data is not None:
        if additional_data == "NULL":
            message.additional_data = {}
        if message.additional_data is None:
            message.additional_data = {}
        for key, value in additional_data.items():
            message.additional_data[key] = value
        flag_modified(message, "additional_data")

    general.flush_or_commit(with_commit)

    return message


def delete(project_id: str, message_id: str, with_commit: bool = True) -> None:
    session.query(CognitionMessage).filter(
        CognitionMessage.project_id == project_id,
        CognitionMessage.id == message_id,
    ).delete()
    general.flush_or_commit(with_commit)


def get_response_time_messages(project_id: str) -> List[Dict[str, Any]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))

    query = f"""
    -- round in ,5 steps
    SELECT ROUND(SUM(x) * 2) /2  AS time_seconds, COUNT(*)
    FROM (
        SELECT m.id, SUM(pl.time_elapsed)
        FROM cognition.message m
        INNER JOIN cognition.conversation c 
            ON c.id = m.conversation_id AND c.project_id = m.project_id AND c.incognito_mode = FALSE
        INNER JOIN cognition.pipeline_logs pl 
            ON m.id = pl.message_id 
        WHERE m.project_id = '{project_id}'
        GROUP BY m.id
    ) x
    GROUP BY time_seconds
    ORDER BY time_seconds
    """
    return general.execute_all(query)


def get_conversations_messages_count(project_id: str) -> List[Dict[str, Any]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    SELECT 
        num_messages,
        num_conversations,
        num_conversations / conv_count.c * 100 percentage
    FROM (
        SELECT COUNT(*) num_conversations, num_messages
        FROM (
            SELECT conversation_id, COUNT(*) num_messages
            FROM cognition.message as m
                JOIN cognition.conversation con ON con.id = m.conversation_id
            WHERE m.project_id = '{project_id}' AND con.incognito_mode = FALSE
            GROUP BY conversation_id
        ) x
        GROUP BY num_messages 
    )x,
    (SELECT COUNT(*)::FLOAT c FROM cognition.conversation WHERE project_id = '{project_id}' AND incognito_mode = FALSE) conv_count
    ORDER BY 1
    """
    return general.execute_all(query)


def get_feedback_distribution(
    project_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None
) -> List[Tuple[str, Any]]:
    where_add = ""

    if start_date and end_date:
        start_date = prevent_sql_injection(start_date, isinstance(start_date, str))
        end_date = prevent_sql_injection(end_date, isinstance(end_date, str))
        where_add += f"AND m.created_at BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        start_date = prevent_sql_injection(start_date, isinstance(start_date, str))
        where_add += f"AND m.created_at >= '{start_date}'"
    elif end_date:
        end_date = prevent_sql_injection(end_date, isinstance(end_date, str))
        where_add += f"AND m.created_at <= '{end_date}'"

    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    SELECT
        feedback_value,
        feedbacks,
        feedbacks / percentage_count.c * 100 percentage
        FROM (
            SELECT COUNT(*) feedbacks, feedback_value
            FROM (
                SELECT feedback_value
                FROM cognition.message m
                    JOIN cognition.conversation con ON con.id = m.conversation_id
                WHERE m.project_id = '{project_id}' AND m.feedback_value IS NOT NULL {where_add} AND con.incognito_mode = FALSE
    )x
    GROUP BY feedback_value
    )x,
    (SELECT COUNT(*)::FLOAT c FROM cognition.message m JOIN cognition.conversation con ON con.id = m.conversation_id WHERE m.project_id = '{project_id}' AND m.feedback_value IS NOT NULL {where_add} AND con.incognito_mode = FALSE) percentage_count
    """
    return general.execute_all(query)


ALLOWED_INTERVALS = {
    "h": "hours",
    "d": "days",
    "w": "weeks",
    "m": "months",
    "y": "years",
}


def __parse_interval(interval: str) -> str:
    split = interval.split(" ")
    if len(split) != 2:
        raise ValueError("Invalid interval format")
    amount = int(split[0])
    unit = split[1]
    if unit not in ALLOWED_INTERVALS and unit not in ALLOWED_INTERVALS.values():
        raise ValueError("Invalid interval format")
    return f"{amount} {ALLOWED_INTERVALS.get(unit, unit)}"


def get_feedback_line_chart_data(
    project_id: str, interval: str, overwrite_group_size: Optional[str] = None
) -> List[Dict[str, Union[str, int]]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    interval = prevent_sql_injection(interval, isinstance(interval, str))
    interval = __parse_interval(interval)

    group_size = "day"
    if overwrite_group_size:
        group_size = prevent_sql_injection(
            overwrite_group_size, isinstance(overwrite_group_size, str)
        )
        if (
            group_size not in ALLOWED_INTERVALS
            and group_size not in ALLOWED_INTERVALS.values()
        ):
            raise ValueError("Invalid interval format")
        group_size = ALLOWED_INTERVALS.get(group_size, group_size)

    query = f"""
    WITH base_select AS (
        SELECT
            date_trunc('{group_size}', M.created_at) time_group,
            feedback_value,
            COUNT(*) c
        FROM cognition.message M
            JOIN cognition.conversation con ON con.id = M.conversation_id
        WHERE M.project_id = '{project_id}'
        AND M.created_at >= CURRENT_TIMESTAMP - INTERVAL '{interval}'
        AND feedback_value IS NOT NULL
        AND con.incognito_mode = FALSE
        GROUP BY 1,2
    )
    SELECT jsonb_object_agg(time_group, vals)
    FROM (
        SELECT
            time_group::TEXT, jsonb_object_agg(feedback_value, c) vals
        FROM base_select bs
        GROUP BY 1
    )x """
    value = general.execute_first(query)
    if value and value[0]:
        return value[0]
    return []


# migration method to be removed after next release
def get_messages_to_be_migrated_to_new_structure() -> List[Tuple[str, str, str]]:
    query = """
    SELECT x.id::TEXT conversation_id,m.id::TEXT message_id, m.scope_dict_diff_previous_conversation
    FROM (
        SELECT DISTINCT c.id, c.project_id
        FROM cognition.conversation c
        INNER JOIN cognition.message m
            ON c.id = m.conversation_id AND c.project_id = m.project_id
        WHERE m.scope_dict_diff_previous_conversation::TEXT != '"null"'
        LIMIT 50 -- max conversations per chunk
    )x
    INNER JOIN cognition.message m
        ON m.conversation_id = x.id AND m.project_id = x.project_id
    ORDER BY m.created_at ASC
    """
    values = general.execute_all(query)
    if values:
        return [(value[0], value[1], value[2]) for value in values]
    return []


def update_to_new_diff_structure(
    message_id: str,
    new_scope_dict_diff: List[Dict[str, Any]],
    with_commit: bool = False,
):
    session.query(CognitionMessage).filter(CognitionMessage.id == message_id).update(
        {
            CognitionMessage.scope_dict_diff_previous_conversation: "null",
            CognitionMessage.scope_dict_diff_new: new_scope_dict_diff,
        },
        synchronize_session=False,
    )

    if with_commit:
        general.commit()


def update_version_id_for_messages(
    project_id: str,
    message_ids: List[str],
    version_id: str,
    with_commit: bool = True,
):
    session.query(CognitionMessage).filter(
        CognitionMessage.project_id == project_id,
        CognitionMessage.id.in_(message_ids),
    ).update(
        {CognitionMessage.version_id: version_id},
        synchronize_session=False,
    )
    if with_commit:
        general.commit()


def get_count_by_project_id(project_id: str) -> int:
    return (
        session.query(CognitionMessage)
        .filter(
            CognitionMessage.project_id == project_id,
        )
        .count()
    )


def get_last_chat_messages(
    message_type: MessageType,
    starting_from: str,
    ending_to: Optional[str] = None,
    cross_selling_filter: Optional[str] = None,
) -> List[Any]:

    message_type = prevent_sql_injection(message_type, isinstance(message_type, str))
    starting_from = prevent_sql_injection(starting_from, isinstance(starting_from, str))
    if ending_to:
        ending_to = prevent_sql_injection(ending_to, isinstance(ending_to, str))
    message_type_filter = ""
    ending_to_filter = ""
    cross_selling_filter_sql = cross_selling_bo.build_cross_selling_filter_sql(
        cross_selling_filter
    )
    if cross_selling_filter_sql:
        cross_selling_filter_sql = " AND " + cross_selling_filter_sql

    if message_type == MessageType.WITH_ERROR:
        message_type_filter = "AND c.error IS NOT NULL"
    elif message_type == MessageType.WITHOUT_ERROR:
        message_type_filter = "AND c.error IS NULL"
    if ending_to:
        ending_to_filter = f"AND m.created_at <= '{ending_to}'"

    query = f"""
    SELECT *
    FROM (
        SELECT m.created_at, m.created_by, m.question, m.answer, m.initiated_via, c.error, cp.id AS project_id, cp.name AS project_name, cp.organization_id, o.name AS organization_name, c.id AS conversation_id, cs.name AS cross_selling_name,
            ROW_NUMBER() OVER (
                PARTITION BY cp.organization_id, cp.id 
                ORDER BY m.created_at DESC
            ) AS rn
        FROM cognition.message m
            JOIN cognition.conversation c ON c.id = m.conversation_id
            JOIN cognition.project cp ON cp.id = m.project_id
            JOIN organization o ON o.id = cp.organization_id
            LEFT JOIN cross_selling cs ON cs.id = o.cross_selling_id
        WHERE 
            m.created_at >= '{starting_from}'
            {message_type_filter}
            {ending_to_filter}
            {cross_selling_filter_sql}
    ) sub
    WHERE rn <= 5
    ORDER BY organization_id, project_id, created_at DESC
    """

    return general.execute_all(query)


def get_last_negative_feedback_per_org(
    created_at_from: str,
    created_at_to: Optional[str] = None,
    cross_selling_filter: Optional[str] = None,
    feedback_category: Optional[str] = None,
) -> List[Any]:
    created_at_from = prevent_sql_injection(
        created_at_from, isinstance(created_at_from, str)
    )
    if created_at_to:
        created_at_to = prevent_sql_injection(
            created_at_to, isinstance(created_at_to, str)
        )
    created_at_to_filter = ""
    cross_selling_filter_sql = cross_selling_bo.build_cross_selling_filter_sql(
        cross_selling_filter
    )
    if cross_selling_filter_sql:
        cross_selling_filter_sql = " AND " + cross_selling_filter_sql

    if created_at_to:
        created_at_to_filter = f"AND m.created_at <= '{created_at_to}'"

    feedback_category_filter = ""
    if feedback_category is not None and feedback_category.strip() != "":
        fc = prevent_sql_injection(feedback_category.strip(), True)
        feedback_category_filter = f"AND m.feedback_category = '{fc}'"

    query = f"""
    SELECT *
    FROM (
        SELECT m.id AS message_id, m.created_at, m.created_by, m.question, m.answer,
            m.feedback_category, m.feedback_message AS "feedbackMessage", m.initiated_via,
            cp.id AS project_id, cp.name AS project_name, cp.organization_id,
            o.name AS organization_name, c.id AS conversation_id, cs.name AS cross_selling_name,
            ROW_NUMBER() OVER (
                PARTITION BY cp.organization_id, cp.id
                ORDER BY m.created_at DESC
            ) AS rn
        FROM cognition.message m
            JOIN cognition.conversation c
                ON c.id = m.conversation_id AND c.project_id = m.project_id
            JOIN cognition.project cp ON cp.id = m.project_id
            JOIN organization o ON o.id = cp.organization_id
            LEFT JOIN cross_selling cs ON cs.id = o.cross_selling_id
        WHERE
            m.feedback_value = 'negative'
            AND c.incognito_mode = FALSE
            AND m.created_at >= '{created_at_from}'
            {created_at_to_filter}
            {cross_selling_filter_sql}
            {feedback_category_filter}
    ) sub
    WHERE rn <= 5
    ORDER BY organization_id, project_id, created_at DESC
    """

    return general.execute_all(query)
