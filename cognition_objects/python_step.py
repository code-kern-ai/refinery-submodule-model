from datetime import datetime
from typing import Dict, List, Optional, Tuple

from . import message
from ..business_objects import general
from ..session import session
from ..models import CognitionPythonStep
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(python_step_id: str) -> CognitionPythonStep:
    return (
        session.query(CognitionPythonStep)
        .filter(CognitionPythonStep.id == python_step_id)
        .first()
    )


def get_python_strategy_step_data(strategy_step_id: str) -> CognitionPythonStep:
    return (
        session.query(CognitionPythonStep)
        .filter(CognitionPythonStep.strategy_step_id == strategy_step_id)
        .first()
    )


def create(
    project_id: str,
    strategy_step_id: str,
    user_id: str,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> CognitionPythonStep:
    source_code = f"""from typing import Dict, Any, Tuple

def routing(record_dict: Dict[str, Any], scope_dict: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return record_dict, scope_dict
"""
    python_step: CognitionPythonStep = CognitionPythonStep(
        project_id=project_id,
        strategy_step_id=strategy_step_id,
        created_by=user_id,
        created_at=created_at,
        source_code=source_code,
    )
    general.add(python_step, with_commit)

    return python_step


def update(
    python_step_id: str,
    source_code: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionPythonStep:
    python_step: CognitionPythonStep = get(python_step_id)

    if source_code:
        python_step.source_code = source_code

    general.flush_or_commit(with_commit)
    return python_step
