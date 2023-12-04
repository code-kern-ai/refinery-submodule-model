from typing import Any, Dict, Optional, List

from ..business_objects import general
from ..session import session
from ..models import CognitionSelectionStep
from datetime import datetime


def get(project_id: str, selection_step_id: str) -> CognitionSelectionStep:
    return (
        session.query(CognitionSelectionStep)
        .filter(
            CognitionSelectionStep.project_id == project_id,
            CognitionSelectionStep.id == selection_step_id,
        )
        .first()
    )


def get_selection_strategy_step_data(
    project_id: str, strategy_step_id: str
) -> CognitionSelectionStep:
    return (
        session.query(CognitionSelectionStep)
        .filter(
            CognitionSelectionStep.project_id == project_id,
            CognitionSelectionStep.strategy_step_id == strategy_step_id,
        )
        .first()
    )


def create(
    project_id: str,
    strategy_step_id: str,
    user_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionSelectionStep:

    selection_step: CognitionSelectionStep = CognitionSelectionStep(
        project_id=project_id,
        strategy_step_id=strategy_step_id,
        created_by=user_id,
        created_at=created_at,
        config=[],
    )
    general.add(selection_step, with_commit)

    return selection_step


def update(
    project_id: str,
    selection_step_id: str,
    config: Optional[List[Dict[str, Any]]] = None,
    with_commit: bool = True,
) -> CognitionSelectionStep:
    selection_step: CognitionSelectionStep = get(project_id, selection_step_id)

    if config:
        selection_step.config = config

    general.flush_or_commit(with_commit)
    return selection_step
