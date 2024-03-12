from typing import Any, Dict, List, Optional
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
    project_id: str, last_x_hours: Optional[int] = None, only_with_feedback: bool = True
) -> str:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    where_add = ""
    if only_with_feedback:
        where_add += "AND (mo.feedback_value IS NOT NULL OR y.has_error)"
    if last_x_hours is not None:
        last_x_hours = prevent_sql_injection(
            last_x_hours, isinstance(last_x_hours, int)
        )
        where_add += f"AND (mo.created_at BETWEEN NOW() - INTERVAL '{last_x_hours} HOURS' AND NOW())"
    return f"""
    SELECT
        COALESCE(feedback_value, CASE WHEN y.has_error THEN 'ERROR_IN_NEWEST_LOG' ELSE NULL END) feedback_value_or_error, 
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
