from typing import List

from ..models import EvaluationGroup
from ..session import session
from . import general


def get(project_id: str, evaluation_group_id: str) -> EvaluationGroup:
    query = session.query(EvaluationGroup).filter(
        EvaluationGroup.project_id == project_id,
        EvaluationGroup.id == evaluation_group_id,
    )
    return query.first()


def get_all(project_id: str) -> List[EvaluationGroup]:
    query = session.query(EvaluationGroup).filter(
        EvaluationGroup.project_id == project_id,
    )
    query = query.order_by(EvaluationGroup.name)
    return query.all()


def create(
    project_id: str,
    name: str,
    created_by: str,
    evaluation_set_ids: List[str],
    with_commit: bool = False,
) -> EvaluationGroup:
    eval_group = EvaluationGroup(
        project_id=project_id,
        name=name,
        created_by=created_by,
        evaluation_set_ids=evaluation_set_ids,
    )

    general.add(eval_group, with_commit)

    return eval_group
