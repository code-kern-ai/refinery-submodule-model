from datetime import datetime
from typing import Dict, List, Optional, Tuple
from ..business_objects import general
from ..session import session
from ..models import Message
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get_all_by_conversation_id(conversation_id: str) -> List[Message]:
    return (
        session.query(Message)
        .filter(Message.conversation_id == conversation_id)
        .order_by(Message.created_at.asc())
        .all()
    )


def get_last_by_conversation_id(conversation_id: str) -> Message:
    return (
        session.query(Message)
        .filter(Message.conversation_id == conversation_id)
        .order_by(Message.created_at.desc())
        .first()
    )


def get(message_id: str) -> Message:
    return session.query(Message).filter(Message.id == message_id).first()


def get_by_strategy_id(strategy_id: str) -> Message:
    return session.query(Message).filter(Message.strategy_id == strategy_id).first()


def create(
    conversation_id: str,
    project_id: str,
    user_id: str,
    content: str,
    role: str,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> Message:
    message = Message(
        project_id=project_id,
        conversation_id=conversation_id,
        created_by=user_id,
        created_at=created_at,
        content=content,
        role=role,
        facts=[],
    )

    general.add(message, with_commit)

    return message


def delete(message_id: str, with_commit: bool = True) -> None:
    session.query(Message).filter(
        Message.id == message_id,
    ).delete()
    general.flush_or_commit(with_commit)
