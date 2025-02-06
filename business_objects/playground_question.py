from typing import List

from ..models import PlaygroundQuestion
from ..session import session
from . import general


MAX_SAVED_QUESTIONS_HISTORY_PER_PROJECT = 100


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
    with_commit: bool = False,
) -> PlaygroundQuestion:

    current_count = (
        session.query(PlaygroundQuestion)
        .filter(
            PlaygroundQuestion.project_id == project_id,
        )
        .count()
    )

    if current_count >= MAX_SAVED_QUESTIONS_HISTORY_PER_PROJECT:
        oldest = (
            session.query(PlaygroundQuestion)
            .filter(
                PlaygroundQuestion.project_id == project_id,
            )
            .order_by(PlaygroundQuestion.created_at.asc())
            .limit(current_count - MAX_SAVED_QUESTIONS_HISTORY_PER_PROJECT + 1)
            .all()
        )
        ids = [q.id for q in oldest]
        delete_all(project_id, ids, False)

    q = PlaygroundQuestion(
        project_id=project_id,
        question=question,
    )

    general.add(q, with_commit)

    return q


def delete_all(project_id: str, ids: List[str], with_commit: bool = False) -> None:
    session.query(PlaygroundQuestion).filter(
        PlaygroundQuestion.project_id == project_id,
        PlaygroundQuestion.id.in_(ids),
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)


def delete(project_id: str, id: str, with_commit: bool = True) -> None:
    session.query(PlaygroundQuestion).filter(
        PlaygroundQuestion.project_id == project_id,
        PlaygroundQuestion.id == id,
    ).delete()
    general.flush_or_commit(with_commit)
