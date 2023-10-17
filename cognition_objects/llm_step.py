from datetime import datetime
from typing import Dict, List, Optional, Tuple

from . import message
from ..business_objects import general
from ..session import session
from ..models import CognitionLLMStep
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(llm_step_id: str) -> CognitionLLMStep:
    return (
        session.query(CognitionLLMStep)
        .filter(CognitionLLMStep.id == llm_step_id)
        .first()
    )


def get_llm_strategy_step_data(strategy_step_id: str) -> CognitionLLMStep:
    return (
        session.query(CognitionLLMStep)
        .filter(CognitionLLMStep.strategy_step_id == strategy_step_id)
        .first()
    )


def create(
    project_id: str,
    strategy_step_id: str,
    user_id: str,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> CognitionLLMStep:
    llm_identifier = "openai"
    template_prompt = "You are an AI assistant."
    llm_config = {}

    llm_step: CognitionLLMStep = CognitionLLMStep(
        project_id=project_id,
        strategy_step_id=strategy_step_id,
        created_by=user_id,
        llm_identifier=llm_identifier,
        template_prompt=template_prompt,
        llm_config=llm_config,
        created_at=created_at,
    )
    general.add(llm_step, with_commit)

    return llm_step


def update(
    llm_step_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionLLMStep:
    python_step: CognitionLLMStep = get(llm_step_id)

    if name:
        python_step.name = name
    if description:
        python_step.description = description

    general.flush_or_commit(with_commit)
    return python_step
