from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from ..business_objects import general
from ..session import session
from ..models import CognitionMessage
from ..util import prevent_sql_injection


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


def get_message_feedback_overview_query(
    project_id: str,
    start_date: Optional[int] = None,
    to_date: Optional[int] = None,
) -> str:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    where_add = ""

    if start_date is not None:
        start_date = prevent_sql_injection(start_date, isinstance(start_date, int))
        if to_date is not None:
            to_date = prevent_sql_injection(to_date, isinstance(to_date, int))
            where_add = f"AND mo.created_at BETWEEN TO_TIMESTAMP({start_date} / 1000.0) AND TO_TIMESTAMP({to_date} / 1000.0)"
        else:
            where_add = f"AND mo.created_at >= TO_TIMESTAMP({start_date} / 1000.0)"
    return f"""
    SELECT
        COALESCE(feedback_value,'ERROR_IN_NEWEST_LOG') feedback_value_or_error, 
        feedback_message, 
        feedback_category,
        question, 
        answer,
        x.full_conversation_text,
        json_build_object(
            'message_id',mo.id,
            'conversation_id',mo.conversation_id,
            'user_id',mo.created_by,
            'message_created', mo.created_at,
            'newest_log_has_error', COALESCE(y.has_error,FALSE),
            'has_error_log_content', ARRAY_TO_STRING( y.content,'\n')
        )::TEXT message_data
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
            WHERE C.project_id = '{project_id}'
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
    AND (mo.feedback_value IS NOT NULL OR y.has_error)
    {where_add}
    ORDER BY mo.created_at DESC"""


def create(
    conversation_id: str,
    project_id: str,
    user_id: str,
    question: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionMessage:
    message = CognitionMessage(
        project_id=project_id,
        conversation_id=conversation_id,
        created_by=user_id,
        created_at=created_at,
        question=question,
        facts=[],
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

    general.flush_or_commit(with_commit)

    return message


def delete(project_id: str, message_id: str, with_commit: bool = True) -> None:
    session.query(CognitionMessage).filter(
        CognitionMessage.project_id == project_id,
        CognitionMessage.id == message_id,
    ).delete()
    general.flush_or_commit(with_commit)


def get_messages_per_conversation_query(project_id: str) -> str:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return f"""
    SELECT 
        conversation_id,
        COUNT(*) messages,
        c."header"
    FROM cognition.message as m
        inner join cognition.conversation c on c.id = m.conversation_id and c.project_id = m.project_id 
    WHERE m.project_id = '{project_id}'
    GROUP BY conversation_id, c."header" 
    ORDER BY conversation_id, c."header"
    """


def get_response_time_messages_query(project_id: str) -> str:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return f"""
    select round(sum(x) * 1000 / 500) * 500 / 1000 AS time_seconds, COUNT(*)
    from (
        select m.id, sum(pl.time_elapsed)
    from cognition.message m
        inner join cognition.conversation c on c.id = m.conversation_id and c.project_id = m.project_id
        inner join cognition.pipeline_logs pl ON m.id = pl.message_id 
    WHERE m.project_id = '{project_id}'
    group by m.id
    ) x
    group by time_seconds
    """


def get_conversations_messages_count_query(project_id: str) -> str:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return f"""
    SELECT 
        COUNT(*) num_conversations,
        num_messages,
        COUNT(*) / (SELECT COUNT(*) FROM cognition.conversation WHERE project_id = '{project_id}')::FLOAT * 100 percentage
    FROM (
        SELECT 
            conversation_id,
            COUNT(*) num_messages
        FROM cognition.message as m
            inner join cognition.conversation c on c.id = m.conversation_id and c.project_id = m.project_id 
        WHERE m.project_id = '{project_id}'
        GROUP BY conversation_id
    ) x
    GROUP BY num_messages
    ORDER BY num_messages
    """


def get_feedback_distribution_query(project_id: str) -> str:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return f"""
    SELECT 
        feedback_value,
        COUNT(*) feedbacks,
        COUNT(*) / (SELECT COUNT(*) FROM cognition.message WHERE project_id = '{project_id}' AND feedback_value IS NOT NULL)::FLOAT * 100 percentage
    FROM cognition.message as m
    WHERE m.project_id = '{project_id}' AND m.feedback_value IS NOT NULL
    GROUP BY feedback_value
    ORDER BY feedback_value
    """


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


def get_feedback_overview_days(
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
            date_trunc('{group_size}', created_at) time_group,
            feedback_value,
            COUNT(*) c
        FROM cognition.message M
        WHERE project_id = '{project_id}'
        AND created_at >= CURRENT_TIMESTAMP - INTERVAL '{interval}'
        AND feedback_value IS NOT NULL
        GROUP BY 1,2
    )
    SELECT array_agg(jsonb_build_object('date', time_group) || vals)
    FROM (
        SELECT
            time_group::TEXT, jsonb_object_agg(vote, C) vals
        FROM (
            SELECT
                COALESCE(bs.time_group,f.time_group) time_group,
                COALESCE(bs.feedback_value,f.gr) vote,
                COALESCE(bs.c,f.c) c
            FROM base_select bs
            RIGHT JOIN (
                SELECT time_group, gr, c
                FROM (
                    SELECT DISTINCT time_group
                    FROM base_select ) g,
                (
                    SELECT 'positive' gr, 0 C
                    UNION ALL SELECT 'negative', 0
                    UNION ALL SELECT 'neutral', 0
                )y
            )f
                ON bs.time_group = f.time_group AND bs.feedback_value = f.gr
        ) x
        GROUP BY 1
    )x """
    value = general.execute_first(query)
    if value and value[0]:
        return value[0]
    return []
