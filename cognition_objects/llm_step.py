from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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
    question_prompt = """User query: {{ record.query }}
<br>
<br>
Add some contextual data here if you want to.
"""
    llm_config = {
        "model": "gpt-3.5-turbo",
        "temperature": 0,
        "maxLength": 1024,
        "stopSequences": [],
        "topP": 1,
        "frequencyPenalty": 0,
        "presencePenalty": 0,
    }

    llm_step: CognitionLLMStep = CognitionLLMStep(
        project_id=project_id,
        strategy_step_id=strategy_step_id,
        created_by=user_id,
        llm_identifier=llm_identifier,
        template_prompt=template_prompt,
        question_prompt=question_prompt,
        llm_config=llm_config,
        created_at=created_at,
    )
    general.add(llm_step, with_commit)

    return llm_step


def update(
    llm_step_id: str,
    llm_config: Optional[Dict[str, Any]] = None,
    template_prompt: Optional[str] = None,
    question_prompt: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionLLMStep:
    python_step: CognitionLLMStep = get(llm_step_id)

    if llm_config is not None:
        python_step.llm_config = llm_config.copy()
    if template_prompt is not None:
        python_step.template_prompt = template_prompt
    if question_prompt is not None:
        python_step.question_prompt = question_prompt

    general.flush_or_commit(with_commit)
    return python_step
