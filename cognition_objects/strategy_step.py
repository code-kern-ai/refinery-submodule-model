from typing import List, Optional, Dict
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
        .order_by(CognitionStrategyStep.position.asc())
        .all()
    )


def create(
    project_id: str,
    strategy_id: str,
    user_id: str,
    name: str,
    description: str,
    step_type: str,
    position: int,
    config: Dict,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionStrategyStep:
    strategy: CognitionStrategyStep = CognitionStrategyStep(
        project_id=project_id,
        strategy_id=strategy_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        step_type=step_type,
        position=position,
        config=config,
    )
    general.add(strategy, with_commit)

    return strategy


def update(
    project_id: str,
    strategy_step_id: str,
    position: Optional[int] = None,
    config: Optional[Dict] = None,
    with_commit: bool = True,
) -> CognitionStrategyStep:
    strategy_step: CognitionStrategyStep = get(project_id, strategy_step_id)

    if position is not None:
        strategy_step.position = position
    if config is not None:
        strategy_step.config = config

    general.flush_or_commit(with_commit)
    return strategy_step


def delete(project_id: str, strategy_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategyStep).filter(
        CognitionStrategyStep.project_id == project_id,
        CognitionStrategyStep.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)
