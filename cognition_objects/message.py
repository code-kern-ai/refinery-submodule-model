from typing import Any, Dict, List, Optional
from datetime import datetime
from ..business_objects import general
from ..session import session
from ..models import CognitionMessage


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
        selection_widget=[],
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
    
    general.add(message, with_commit)

    return message


def delete(project_id: str, message_id: str, with_commit: bool = True) -> None:
    session.query(CognitionMessage).filter(
        CognitionMessage.project_id == project_id,
        CognitionMessage.id == message_id,
    ).delete()
    general.flush_or_commit(with_commit)
