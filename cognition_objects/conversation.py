from datetime import datetime
from typing import Dict, List, Optional, Tuple

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
    initial_message: str,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> Conversation:
    conversation: Conversation = Conversation(
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
    )
    general.add(conversation, with_commit)

    add_message(
        conversation_id=conversation.id,
        content=initial_message,
        role=enums.MessageRoles.USER.value,
        with_commit=with_commit,
    )

    return conversation


def add_message(
    conversation_id: str,
    content: str,
    role: str,
    with_commit: bool = True,
) -> Conversation:
    conversation: Conversation = get(conversation_id)
    project: CognitionProject = cognition_project.get(conversation.project_id)

    query_type = None
    query_type_confidence = None

    # pipeline
    print("calling gates project", flush=True)

    if role == enums.MessageRoles.USER.value:
        enrichment_response = call_gates_project(
            project_id=project.refinery_query_project_id,
            record_dict={
                "query": content,
            },
        )
        print("gates project called", flush=True)
        if enrichment_response.status_code == 200:
            # {'record': {'query': 'hi'}, 'results': {'Question Type': {'prediction': 'explorative', 'confidence': 0.9820137900379085, 'heuristics': [{'name': 'my_labeling_function', 'prediction': 'explorative', 'confidence': 1.0}]}}}
            enrichment_response_json = enrichment_response.json()
            print(enrichment_response_json, flush=True)
            question_type = enrichment_response_json["results"].get("Question Type")
            if question_type:
                query_type = question_type["prediction"]
                query_type_confidence = question_type["confidence"]
        else:
            print(enrichment_response.text, flush=True)

    message.create(
        conversation_id=conversation_id,
        project_id=conversation.project_id,
        user_id=conversation.created_by,
        content=content,
        role=role,
        query_type=query_type,
        query_type_confidence=query_type_confidence,
        with_commit=with_commit,
    )
    return conversation


def delete(conversation_id: str, with_commit: bool = True) -> None:
    session.query(Conversation).filter(
        Conversation.id == conversation_id,
    ).delete()
    general.flush_or_commit(with_commit)
