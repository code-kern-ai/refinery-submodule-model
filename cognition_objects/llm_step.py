from typing import Any, Dict, Optional

from ..business_objects import general
from ..session import session
from ..models import CognitionLLMStep
from .. import enums


def get(project_id: str, llm_step_id: str) -> CognitionLLMStep:
    return (
        session.query(CognitionLLMStep)
        .filter(
            CognitionLLMStep.project_id == project_id,
            CognitionLLMStep.id == llm_step_id,
        )
        .first()
    )


def get_first_from_project(project_id: str) -> CognitionLLMStep:
    # only viable directly after project creation since this returns the first (and only) step
    query = f"""
    SELECT cls.*, css.strategy_id
    FROM cognition.llm_step cls
    INNER JOIN cognition.strategy_step css 
        ON cls.project_id = css.project_id AND cls.strategy_step_id = css.id
    WHERE cls.project_id = '{project_id}'
    LIMIT 1
    """
    return general.execute_first(query)


def get_llm_strategy_step_data(
    project_id: str, strategy_step_id: str
) -> CognitionLLMStep:
    return (
        session.query(CognitionLLMStep)
        .filter(
            CognitionLLMStep.project_id == project_id,
            CognitionLLMStep.strategy_step_id == strategy_step_id,
        )
        .first()
    )


def create(
    project_id: str,
    strategy_step_id: str,
    user_id: str,
    with_commit: bool = True,
    usage_type: Optional[str] = None,
    created_at: Optional[str] = None,
) -> CognitionLLMStep:
    llm_identifier = "Open AI"    
    llm_config = {
        "model": "gpt-3.5-turbo",
        "temperature": 0,
        "maxLength": 1024,
        "stopSequences": [],
        "topP": 1,
        "frequencyPenalty": 0,
        "presencePenalty": 0,
    }
    if usage_type is None:
        usage_type = enums.CognitionLLMStepUsageType.BASE.value

    if usage_type == enums.CognitionLLMStepUsageType.BASE.value:
        template_prompt = "You are an AI assistant."
        question_prompt = """User question: {{ record.question }} <br><br>
Please use the following references to answer the question:<br>
{{#retrieval_results}}
    **{{name}}**
    <br>{{reference}}<br>
   ----------
{{/retrieval_results}}
"""
    elif usage_type == enums.CognitionLLMStepUsageType.QUERY_REPHRASING.value:
        template_prompt = "Rephrase the user question to work as a google search query."
        question_prompt = """User question: {{ record.question }}
Previous response: {{record.answer_prev_1}}
Previous question: {{record.answer_prev_1}}
Previous response: {{record.answer_prev_2}}
Previous question: {{record.answer_prev_2}}
Previous response: {{record.answer_prev_3}}
Previous question: {{record.answer_prev_3}}
"""
    else:
        raise ValueError(f"Unknown usage type {usage_type}")

    llm_step: CognitionLLMStep = CognitionLLMStep(
        project_id=project_id,
        strategy_step_id=strategy_step_id,
        created_by=user_id,
        llm_identifier=llm_identifier,
        template_prompt=template_prompt,
        question_prompt=question_prompt,
        llm_config=llm_config,
        created_at=created_at,
        usage_type=usage_type,
    )
    general.add(llm_step, with_commit)

    return llm_step


def update(
    project_id: str,
    llm_step_id: str,
    llm_identifier: Optional[str] = None,
    llm_config: Optional[Dict[str, Any]] = None,
    template_prompt: Optional[str] = None,
    question_prompt: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionLLMStep:
    python_step: CognitionLLMStep = get(project_id, llm_step_id)
    if llm_identifier is not None:
        python_step.llm_identifier = llm_identifier
    if llm_config is not None:
        python_step.llm_config = llm_config.copy()
    if template_prompt is not None:
        python_step.template_prompt = template_prompt
    if question_prompt is not None:
        python_step.question_prompt = question_prompt

    general.flush_or_commit(with_commit)
    return python_step
