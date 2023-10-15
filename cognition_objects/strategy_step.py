from datetime import datetime
from typing import Dict, List, Optional, Tuple

from . import message
from ..business_objects import general
from ..session import session
from ..models import CognitionStrategyStep, CognitionPythonStep
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(strategy_id: str) -> CognitionStrategyStep:
    return (
        session.query(CognitionStrategyStep)
        .filter(CognitionStrategyStep.id == strategy_id)
        .first()
    )


def get_all_by_strategy_id(strategy_id: str) -> List[CognitionStrategyStep]:
    return (
        session.query(CognitionStrategyStep)
        .filter(CognitionStrategyStep.strategy_id == strategy_id)
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
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> CognitionStrategyStep:
    strategy: CognitionStrategyStep = CognitionStrategyStep(
        project_id=project_id,
        strategy_id=strategy_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        strategy_step_type=strategy_step_type,
        strategy_step_position=strategy_step_position,
    )
    general.add(strategy, with_commit)

    return strategy


def update(
    strategy_step_id: str,
    strategy_step_position: Optional[int] = None,
    with_commit: bool = True,
) -> CognitionStrategyStep:
    strategy_step: CognitionStrategyStep = get(strategy_step_id)

    if strategy_step_position is not None:
        strategy_step.strategy_step_position = strategy_step_position

    general.flush_or_commit(with_commit)
    return strategy_step


def delete(strategy_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategyStep).filter(
        CognitionStrategyStep.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)
