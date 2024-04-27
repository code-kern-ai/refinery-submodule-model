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


def delete_all_by_project_id(
    project_id: str,
    with_commit: bool = True,
) -> None:
    session.query(CognitionAction).filter(
        CognitionAction.project_id == project_id,
    ).delete()

    general.flush_or_commit(with_commit)
