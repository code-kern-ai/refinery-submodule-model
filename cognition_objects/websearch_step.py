from typing import Any, Dict, Optional

from ..business_objects import general
from ..session import session
from ..models import CognitionWebsearchStep
from .. import enums


def get(project_id: str, llm_step_id: str) -> CognitionWebsearchStep:
    return (
        session.query(CognitionWebsearchStep)
        .filter(
            CognitionWebsearchStep.project_id == project_id,
            CognitionWebsearchStep.id == llm_step_id,
        )
        .first()
    )



def get_websearch_strategy_step_data(
    project_id: str, strategy_step_id: str
) -> CognitionWebsearchStep:
    return (
        session.query(CognitionWebsearchStep)
        .filter(
            CognitionWebsearchStep.project_id == project_id,
            CognitionWebsearchStep.strategy_step_id == strategy_step_id,
        )
        .first()
    )


def create(
    project_id: str,
    strategy_step_id: str,
    user_id: str,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> CognitionWebsearchStep:
    
    llm_step: CognitionWebsearchStep = CognitionWebsearchStep(
        project_id=project_id,
        strategy_step_id=strategy_step_id,
        created_by=user_id,
        config={
            'api_key': None,
            'engine': 'google',
            'q': '{{question_rephrased_for_search}}',
            'location': 'Germany',
            'google_domain': 'google.com',
            'gl': 'de',
            'hl': 'de'
        },
        created_at=created_at,
    )
    general.add(llm_step, with_commit)

    return llm_step


def update(
    project_id: str,
    websearch_step_id: str,
    config: Optional[Dict[str, Any]] = None,
    with_commit: bool = True,
) -> CognitionWebsearchStep:
    websearch_step: CognitionWebsearchStep = get(project_id, websearch_step_id)
    if config is not None:
        websearch_step.config = config

    general.flush_or_commit(with_commit)
    return websearch_step
