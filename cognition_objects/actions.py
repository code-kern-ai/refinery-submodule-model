from typing import List, Optional, Dict, Any
from ..business_objects import general
from ..cognition_objects import project
from ..session import session
from ..models import CognitionAction
from datetime import datetime


def get_all_by_project_id(
    project_id: str,
) -> List[CognitionAction]:
    return (
        session.query(CognitionAction)
        .filter(
            CognitionAction.project_id == project_id,
            )
        .order_by(CognitionAction.created_at.asc())
        .all()
    )

def get(
    project_id: str,
    action_id: str,
) -> CognitionAction:
    return (
        session.query(CognitionAction)
        .filter(
            CognitionAction.project_id == project_id,
            CognitionAction.id == action_id,
        )
        .first()
    )


def create(
    project_id: str,
    user_id: str,
    name: str,
    description: str,
    questions: Dict[str, Any],
    on_enter_send_message: Optional[bool] = False,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionAction:
    
    project_entity = project.get(project_id)

    action = CognitionAction(
        project_id=project_entity.id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        questions=questions,
        on_enter_send_message=on_enter_send_message,
    )
    general.add(action, with_commit)
    return action


def update(
    project_id: str,
    action_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    questions: Optional[Dict[str, Any]] = None,
    on_enter_send_message: Optional[bool] = None,
    with_commit: bool = True,
) -> CognitionAction:
    action = get(project_id, action_id)

    if name is not None:
        action.name = name
    if description is not None:
        action.description = description
    if questions is not None:
        action.questions = questions
    if on_enter_send_message is not None:
        action.on_enter_send_message = on_enter_send_message

    general.add(action, with_commit)
    return action


def delete_by_ids(
    project_id: str,
    ids: List[str],
    with_commit: bool = True,
) -> None:
    session.query(CognitionAction).filter(
        CognitionAction.project_id == project_id,
        CognitionAction.id.in_(ids),
    ).delete()

    general.flush_or_commit(with_commit)
