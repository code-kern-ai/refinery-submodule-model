from typing import List, Optional
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionStrategyStep


def get(project_id: str, strategy_step_id: str) -> CognitionStrategyStep:
    return (
        session.query(CognitionStrategyStep)
        .filter(
            CognitionStrategyStep.project_id == project_id,
            CognitionStrategyStep.id == strategy_step_id,
        )
        .first()
    )


def get_all_by_strategy_id(
    project_id: str, strategy_id: str
) -> List[CognitionStrategyStep]:
    return (
        session.query(CognitionStrategyStep)
        .filter(
            CognitionStrategyStep.project_id == project_id,
            CognitionStrategyStep.strategy_id == strategy_id,
        )
        .order_by(CognitionStrategyStep.strategy_step_position.asc())
        .all()
    )


def create(
    project_id: str,
    strategy_id: str,
    user_id: str,
    name: str,
    description: str,
    strategy_step_type: str,
    strategy_step_position: int,
    progress_text: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionStrategyStep:
    
    execute_if_source_code = """def check_execute(record_dict, scope_dict):
    return True
"""

    strategy: CognitionStrategyStep = CognitionStrategyStep(
        project_id=project_id,
        strategy_id=strategy_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        progress_text=progress_text,
        strategy_step_type=strategy_step_type,
        strategy_step_position=strategy_step_position,
        execute_if_source_code=execute_if_source_code,
    )
    general.add(strategy, with_commit)

    return strategy


def update(
    project_id: str,
    strategy_step_id: str,
    strategy_step_position: Optional[int] = None,
    execute_if_source_code: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionStrategyStep:
    strategy_step: CognitionStrategyStep = get(project_id, strategy_step_id)

    if strategy_step_position is not None:
        strategy_step.strategy_step_position = strategy_step_position
    if execute_if_source_code is not None:
        strategy_step.execute_if_source_code = execute_if_source_code

    general.flush_or_commit(with_commit)
    return strategy_step


def delete(project_id: str, strategy_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategyStep).filter(
        CognitionStrategyStep.project_id == project_id,
        CognitionStrategyStep.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)
