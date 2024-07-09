from typing import List, Dict, Any
from ..business_objects import general

from ..models import (
    DataManagerBusinessModels,
    DataManagerBusinessModelQuestions,
    DataManagerDataConcepts,
)
from submodules.model.util import sql_alchemy_to_dict


def create_business_model(
    org_id: str,
    name: str,
    description: str,
    question_ids: List[str],
    llm_config: Dict[str, Any],
    with_commit: bool = False,
) -> Dict[str, Any]:

    bm = DataManagerBusinessModels(
        org_id=org_id,
        name=name,
        description=description,
        llm_config=llm_config,
    )

    general.add(bm, False)

    # TODO: created but not returned
    questions = []
    for q_id in question_ids:
        q = DataManagerBusinessModelQuestions(business_model_id=bm.id, question_id=q_id)
        general.add(q, False)
        questions.append(q)
    general.flush_or_commit(with_commit)

    bm_dict = {**sql_alchemy_to_dict(bm), "questions": sql_alchemy_to_dict(questions)}
    return bm_dict


def create_data_concept(
    business_model_id: str,
    created_by: str,
    question_id: str,
    input: str,
    with_commit: bool = False,
) -> Dict[str, Any]:

    dc = DataManagerDataConcepts(
        business_model_id=business_model_id,
        question_id=question_id,
        created_by=created_by,
        input=input,
    )
    general.add(dc, with_commit)
    return dc
