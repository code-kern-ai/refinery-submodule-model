from datetime import datetime
from typing import Dict, List, Optional, Tuple

from . import message
from ..business_objects import general
from ..session import session
from ..models import StrategyStep
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(strategy_id: str) -> StrategyStep:
    return session.query(StrategyStep).filter(StrategyStep.id == strategy_id).first()


def get_all_by_strategy_id(strategy_id: str) -> List[StrategyStep]:
    return (
        session.query(StrategyStep)
        .filter(StrategyStep.strategy_id == strategy_id)
        .order_by(StrategyStep.created_at.asc())
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
) -> StrategyStep:
    strategy: StrategyStep = StrategyStep(
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


def delete(strategy_id: str, with_commit: bool = True) -> None:
    session.query(StrategyStep).filter(
        StrategyStep.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)
