from datetime import datetime
from typing import Dict, List, Optional, Tuple
from src.controller import pipeline
from src.controller.business_objects.project.gates import call_gates_project

from ..cognition_objects import message, project as cognition_project
from ..business_objects import general
from ..session import session
from ..models import Conversation, CognitionProject
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(conversation_id: str) -> Conversation:
    return (
        session.query(Conversation).filter(Conversation.id == conversation_id).first()
    )


def get_all_by_project_id(project_id: str) -> List[Conversation]:
    return (
        session.query(Conversation)
        .filter(Conversation.project_id == project_id)
        .order_by(Conversation.created_at.asc())
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> Conversation:
    conversation: Conversation = Conversation(
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
    )
    general.add(conversation, with_commit)
    return conversation


def add_message(
    conversation_id: str,
    content: str,
    role: str,
    with_commit: bool = True,
) -> Conversation:
    conversation_entity: Conversation = get(conversation_id)

    message.create(
        conversation_id=conversation_id,
        project_id=conversation_entity.project_id,
        user_id=conversation_entity.created_by,
        content=content,
        role=role,
        with_commit=with_commit,
    )

    return conversation_entity


def update_message(
    conversation_id: str,
    message_id: str,
    strategy_id: Optional[str] = None,
    with_commit: bool = True,
) -> Conversation:
    message_entity = message.get(message_id)
    if strategy_id is not None:
        message_entity.strategy_id = strategy_id
    general.flush_or_commit(with_commit)
    conversation_entity = get(conversation_id)
    return conversation_entity


def delete(conversation_id: str, with_commit: bool = True) -> None:
    session.query(Conversation).filter(
        Conversation.id == conversation_id,
    ).delete()
    general.flush_or_commit(with_commit)
