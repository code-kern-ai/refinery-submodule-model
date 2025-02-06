from typing import List

from submodules.model.util import prevent_sql_injection

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


def get_by_evaluation_group_id(
    project_id: str, evaluation_group_id: str
) -> List[EvaluationSet]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    evaluation_group_id = prevent_sql_injection(
        evaluation_group_id, isinstance(evaluation_group_id, str)
    )

    query = f"""
    SELECT es.*
    FROM evaluation_group eg
    JOIN LATERAL jsonb_array_elements_text(eg.evaluation_set_ids::jsonb) AS elem(evaluation_set_id) ON TRUE
    JOIN evaluation_set es ON es.id = elem.evaluation_set_id::uuid
    WHERE eg.project_id = '{project_id}'
    AND eg.id = '{evaluation_group_id}'
    AND es.project_id = '{project_id}'
    ORDER BY es.question;
    """

    return general.execute_all(query)


def create(
    project_id: str,
    question: str,
    created_by: str,
    record_ids: List[str],
    with_commit: bool = False,
) -> EvaluationSet:
    eval_set = EvaluationSet(
        project_id=project_id,
        question=question,
        created_by=created_by,
        record_ids=record_ids,
    )
    general.add(eval_set, with_commit)
    return eval_set


def delete_all(project_id: str, set_ids: List[str], with_commit: bool = False) -> None:
    session.query(EvaluationSet).filter(
        EvaluationSet.project_id == project_id,
        EvaluationSet.id.in_(set_ids),
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)
