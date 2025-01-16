from typing import List, Optional

from submodules.model.enums import EvaluationRunState

from ..models import EvaluationRun
from ..session import session
from . import general


def get(project_id: str, evaluation_run_id: str) -> EvaluationRun:
    query = session.query(EvaluationRun).filter(
        EvaluationRun.project_id == project_id,
        EvaluationRun.id == evaluation_run_id,
    )
    return query.first()


def get_all_by_embedding_id(project_id: str, embedding_id: str) -> EvaluationRun:
    query = session.query(EvaluationRun).filter(
        EvaluationRun.project_id == project_id,
        EvaluationRun.embedding_id == embedding_id,
    )
    query = query.order_by(EvaluationRun.created_at.asc())
    return query.all()


def get_all_by_evaluation_group_id(
    project_id: str, evaluation_group_id: str
) -> EvaluationRun:
    query = session.query(EvaluationRun).filter(
        EvaluationRun.project_id == project_id,
        EvaluationRun.evaluation_group_id == evaluation_group_id,
    )
    query = query.order_by(EvaluationRun.created_at.asc())
    return query.all()


def get_all(project_id: str) -> List[EvaluationRun]:
    query = session.query(EvaluationRun).filter(
        EvaluationRun.project_id == project_id,
    )
    query = query.order_by(EvaluationRun.created_at.asc())
    return query.all()


def create(
    project_id: str,
    evaluation_group_id: str,
    created_by: str,
    embedding_id: str,
    state: EvaluationRunState,
    results: Optional[str] = None,
    meta_info: Optional[str] = None,
    with_commit: bool = False,
) -> EvaluationRun:
    eval_run = EvaluationRun(
        evaluation_group_id=evaluation_group_id,
        created_by=created_by,
        project_id=project_id,
        embedding_id=embedding_id,
        state=state,
    )

    if results is not None:
        eval_run.results = results

    if meta_info is not None:
        eval_run.meta_info = meta_info

    general.add(eval_run, with_commit)

    return eval_run


def update(
    project_id: str,
    evaluation_run_id: str,
    state: EvaluationRunState,
    results: Optional[str] = None,
    meta_info: Optional[str] = None,
    with_commit: bool = False,
) -> EvaluationRun:
    eval_run: EvaluationRun = get(project_id, evaluation_run_id)
    if state is not None:
        eval_run.state = state
    if results is not None:
        eval_run.results = results
    if meta_info is not None:
        eval_run.meta_info = meta_info

    general.flush_or_commit(with_commit)
    return eval_run
