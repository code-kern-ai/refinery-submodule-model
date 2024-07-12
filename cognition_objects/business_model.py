from typing import List, Dict, Any

from ..business_objects import general
from ..session import session

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
        organization_id=org_id,
        name=name,
        description=description,
        llm_config=llm_config,
    )

    general.add(bm, False)

    questions = []
    for q_id in question_ids:
        q = DataManagerBusinessModelQuestions(business_model_id=bm.id, question_id=q_id)
        general.add(q, False)
        questions.append(q)
    general.flush_or_commit(with_commit)

    bm_dict = {**sql_alchemy_to_dict(bm), "questions": sql_alchemy_to_dict(questions)}
    return bm_dict


def get_business_model_by_id(
    org_id: str, business_model_id: str
) -> DataManagerBusinessModels:
    return (
        session.query(DataManagerBusinessModels)
        .filter(
            DataManagerBusinessModels.organization_id == org_id,
            DataManagerBusinessModels.id == business_model_id,
        )
        .first()
    )


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


def get_data_concepts_by_ids(
    data_concept_ids: List[str],
) -> List[DataManagerDataConcepts]:
    return (
        session.query(DataManagerDataConcepts)
        .filter(DataManagerDataConcepts.id.in_(data_concept_ids))
        .all()
    )


def get_all(org_id: str) -> List[DataManagerBusinessModels]:
    return (
        session.query(DataManagerBusinessModels)
        .filter(DataManagerBusinessModels.organization_id == org_id)
        .all()
    )


def get_questions_for_business_model(
    org_id: str, business_model_id: str
) -> List[DataManagerBusinessModelQuestions]:

    query = f"""
    SELECT * 
    FROM data_manager.business_model_questions bmq 
        JOIN data_manager.organization_questions oq on oq.id = bmq.question_id 
    WHERE bmq.business_model_id = '{business_model_id}' and oq.organization_id = '{org_id}'
    """

    return general.execute_all(query)


def get_data_concepts_by_user(
    user_id: str, business_model_id: str
) -> List[DataManagerBusinessModelQuestions]:

    return (
        session.query(DataManagerDataConcepts)
        .join(
            DataManagerBusinessModelQuestions,
            DataManagerBusinessModelQuestions.question_id
            == DataManagerDataConcepts.question_id,
        )
        .filter(DataManagerDataConcepts.business_model_id == business_model_id)
        .filter(DataManagerDataConcepts.created_by == user_id)
        .all()
    )
