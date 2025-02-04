from typing import List, Optional

from ..models import PlaygroundQuestion
from ..session import session
from . import general


def get(project_id: str, question_id: str) -> PlaygroundQuestion:
    query = session.query(PlaygroundQuestion).filter(
        PlaygroundQuestion.project_id == project_id,
        PlaygroundQuestion.id == question_id,
    )
    return query.first()


def get_all(project_id: str) -> List[PlaygroundQuestion]:
    query = session.query(PlaygroundQuestion).filter(
        PlaygroundQuestion.project_id == project_id,
    )
    query = query.order_by(PlaygroundQuestion.created_at.asc())
    return query.all()


def create(
    project_id: str,
    question: str,
    created_by: str,
    embedding_id: str,
    record_ids: List[str],
    meta_info: Optional[str] = None,
    with_commit: bool = False,
) -> PlaygroundQuestion:
    q = PlaygroundQuestion(
        project_id=project_id,
        question=question,
        created_by=created_by,
        embedding_id=embedding_id,
        record_ids=record_ids,
    )

    if meta_info is not None:
        q.meta_info = meta_info

    general.add(q, with_commit)

    return q


def delete_all(project_id: str, set_ids: List[str], with_commit: bool = False) -> None:
    session.query(PlaygroundQuestion).filter(
        PlaygroundQuestion.project_id == project_id,
        PlaygroundQuestion.id.in_(set_ids),
    ).delete(synchronize_session=False)
    if with_commit:
        session.commit()
