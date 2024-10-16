from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm.attributes import flag_modified

from ..business_objects import general
from ..session import session
from ..models import CognitionStrategyStep
from ..enums import StrategyStepType


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


def get_first_llm_step_from_project(project_id: str) -> CognitionStrategyStep:
    # only viable directly after project creation since this returns the first (and only) step
    return (
        session.query(CognitionStrategyStep)
        .filter(
            CognitionStrategyStep.project_id == project_id,
            CognitionStrategyStep.step_type == StrategyStepType.LLM.value,
        )
        .first()
    )


EXECUTE_IF_SOURCE_CODE = """from typing import Dict, Any, Tuple

def check_execute(
    record_dict: Dict[str, Any], scope_dict: Dict[str, Any]
) -> bool:
    return True
"""


def create(
    project_id: str,
    strategy_id: str,
    user_id: str,
    name: str,
    description: str,
    step_type: str,
    position: int,
    config: Dict,
    progress_text: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    execute_if_source_code: Optional[str] = None,
) -> CognitionStrategyStep:
    if not execute_if_source_code:
        execute_if_source_code = EXECUTE_IF_SOURCE_CODE
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
        progress_text=progress_text,
        execute_if_source_code=execute_if_source_code,
    )
    general.add(strategy, with_commit)

    return strategy


def update(
    project_id: str,
    strategy_step_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    position: Optional[int] = None,
    config: Optional[Dict] = None,
    progress_text: Optional[str] = None,
    execute_if_source_code: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionStrategyStep:
    strategy_step: CognitionStrategyStep = get(project_id, strategy_step_id)

    if name is not None:
        strategy_step.name = name
    if description is not None:
        strategy_step.description = description
    if position is not None:
        strategy_step.position = position
    if config is not None:
        if not strategy_step.config:
            strategy_step.config = {}
        for key in config:
            strategy_step.config[key] = config[key]
        flag_modified(strategy_step, "config")
    if progress_text is not None:
        strategy_step.progress_text = progress_text
    if execute_if_source_code is not None:
        strategy_step.execute_if_source_code = execute_if_source_code

    general.flush_or_commit(with_commit)
    return strategy_step


def update_or_insert_config_value(
    project_id: str,
    strategy_step_id: str,
    configKey: str,
    configValue: Any,  # also None!
    with_commit: bool = True,
) -> CognitionStrategyStep:
    strategy_step: CognitionStrategyStep = get(project_id, strategy_step_id)
    if not strategy_step:
        raise ValueError(f"Strategy step with id {strategy_step_id} not found")

    if not strategy_step.config:
        strategy_step.config = {}
    strategy_step.config[configKey] = configValue
    flag_modified(strategy_step, "config")

    general.flush_or_commit(with_commit)
    return strategy_step


def delete(project_id: str, strategy_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategyStep).filter(
        CognitionStrategyStep.project_id == project_id,
        CognitionStrategyStep.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)
