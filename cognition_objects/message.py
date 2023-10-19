from typing import List, Optional
from datetime import datetime
from ..business_objects import general
from ..session import session
from ..models import CognitionMessage


def get_all_by_conversation_id(conversation_id: str) -> List[CognitionMessage]:
    return (
        session.query(CognitionMessage)
        .filter(CognitionMessage.conversation_id == conversation_id)
        .order_by(CognitionMessage.created_at.asc())
        .all()
    )


def get_last_by_conversation_id(conversation_id: str) -> CognitionMessage:
    return (
        session.query(CognitionMessage)
        .filter(CognitionMessage.conversation_id == conversation_id)
        .order_by(CognitionMessage.created_at.desc())
        .first()
    )


def get_last_n_by_conversation_id(
    conversation_id: str, n: int
) -> List[CognitionMessage]:
    return (
        session.query(CognitionMessage)
        .filter(CognitionMessage.conversation_id == conversation_id)
        .order_by(CognitionMessage.created_at.desc())
        .limit(n)
        .all()
    )


def get(message_id: str) -> CognitionMessage:
    return (
        session.query(CognitionMessage)
        .filter(CognitionMessage.id == message_id)
        .first()
    )


def get_by_strategy_id(strategy_id: str) -> CognitionMessage:
    return (
        session.query(CognitionMessage)
        .filter(CognitionMessage.strategy_id == strategy_id)
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
    )

    general.add(message, with_commit)

    return message


def delete(message_id: str, with_commit: bool = True) -> None:
    session.query(CognitionMessage).filter(
        CognitionMessage.id == message_id,
    ).delete()
    general.flush_or_commit(with_commit)
