from typing import List

from ..models import EvaluationSet
from ..session import session
from . import general


def get(project_id: str, evaluation_set_id: str) -> EvaluationSet:
    query = session.query(EvaluationSet).filter(
        EvaluationSet.project_id == project_id,
        EvaluationSet.id == evaluation_set_id,
    )
    return query.first()


def get_all(project_id: str) -> List[EvaluationSet]:
    query = session.query(EvaluationSet).filter(
        EvaluationSet.project_id == project_id,
    )
    query = query.order_by(EvaluationSet.question)
    return query.all()


def create(
    project_id: str,
    question: str,
    created_by: str,
    name: str,
    record_ids: List[str],
    with_commit: bool = False,
) -> EvaluationSet:
    eval_set = EvaluationSet(
        project_id=project_id,
        question=question,
        created_by=created_by,
        name=name,
        record_ids=record_ids,
    )

    general.add(eval_set, with_commit)

    return eval_set
