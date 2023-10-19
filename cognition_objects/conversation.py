from typing import Dict, List, Optional, Tuple, Any

from datetime import datetime

from ..cognition_objects import message
from ..business_objects import general
from ..session import session
from ..models import CognitionConversation


def get(conversation_id: str) -> CognitionConversation:
    return (
        session.query(CognitionConversation)
        .filter(CognitionConversation.id == conversation_id)
        .first()
    )


def get_all_paginated_by_project_id(
    project_id: str, page: int, limit: int
) -> Tuple[int, int, List[CognitionConversation]]:
    total_count = (
        session.query(CognitionConversation.id)
        .filter(CognitionConversation.project_id == project_id)
        .count()
    )

    if total_count == 0:
        num_pages = 0
    else:
        num_pages = int(total_count / limit)
        if total_count % limit > 0:
            num_pages += 1

    if page > 0:
        paginated_result = (
            session.query(CognitionConversation)
            .filter(CognitionConversation.project_id == project_id)
            .order_by(CognitionConversation.created_at.asc())
            .limit(limit)
            .offset((page - 1) * limit)
            .all()
        )
    else:
        paginated_result = []
    return total_count, num_pages, paginated_result


def create(
    project_id: str,
    user_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionConversation:
    conversation: CognitionConversation = CognitionConversation(
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        scope_dict={},
    )
    general.add(conversation, with_commit)
    return conversation


def add_message(
    conversation_id: str,
    query: str,
    with_commit: bool = True,
) -> CognitionConversation:
    conversation_entity: CognitionConversation = get(conversation_id)

    message.create(
        conversation_id=conversation_id,
        project_id=conversation_entity.project_id,
        user_id=conversation_entity.created_by,
        query=query,
        with_commit=with_commit,
    )

    return conversation_entity


def update(
    conversation_id: str,
    scope_dict: Optional[Dict[str, Any]] = None,
    with_commit: bool = True,
) -> CognitionConversation:
    conversation_entity = get(conversation_id)
    if scope_dict is not None:
        conversation_entity.scope_dict = scope_dict
    general.flush_or_commit(with_commit)
    return conversation_entity


def update_message(
    conversation_id: str,
    message_id: str,
    answer: Optional[str] = None,
    strategy_id: Optional[str] = None,
    scope_dict_diff_previous_conversation: Optional[Dict[str, Any]] = None,
    with_commit: bool = True,
) -> CognitionConversation:
    message_entity = message.get(message_id)
    if strategy_id is not None:
        message_entity.strategy_id = strategy_id
    if answer is not None:
        message_entity.answer = answer
    if scope_dict_diff_previous_conversation is not None:
        message_entity.scope_dict_diff_previous_conversation = (
            scope_dict_diff_previous_conversation
        )
    general.flush_or_commit(with_commit)
    conversation_entity = get(conversation_id)
    return conversation_entity


def delete(conversation_id: str, with_commit: bool = True) -> None:
    session.query(CognitionConversation).filter(
        CognitionConversation.id == conversation_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_many(conversation_ids: List[str], with_commit: bool = True) -> None:
    session.query(CognitionConversation).filter(
        CognitionConversation.id.in_(conversation_ids),
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)