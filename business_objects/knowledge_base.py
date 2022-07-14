from typing import List, List, Optional

from ..models import KnowledgeBase
from ..exceptions import EntityAlreadyExistsException, EntityNotFoundException
from ..business_objects import knowledge_term, general
from ..session import session


def get(project_id: str, base_id: str) -> KnowledgeBase:
    return (
        session.query(KnowledgeBase)
        .filter(KnowledgeBase.project_id == project_id, KnowledgeBase.id == base_id)
        .first()
    )



def get_all_by_project_id(project_id: str) -> List[KnowledgeBase]:
    return (
        session.query(KnowledgeBase)
        .filter(KnowledgeBase.project_id == project_id)
        .all()
    )


def get_all(project_id: str) -> List[KnowledgeBase]:
    return (
        session.query(KnowledgeBase)
        .filter(KnowledgeBase.project_id == project_id)
        .all()
    )


def get_by_name(project_id: str, name: str) -> KnowledgeBase:
    return (
        session.query(KnowledgeBase)
        .filter(KnowledgeBase.project_id == project_id, KnowledgeBase.name == name)
        .first()
    )


def get_by_term(project_id: str, term_id: str) -> KnowledgeBase:
    term = knowledge_term.get_by_id(term_id)
    if not term:
        raise EntityNotFoundException
    return get(project_id, term.knowledge_base_id)


def count(project_id: str) -> int:
    return (
        session.query(KnowledgeBase)
        .filter(KnowledgeBase.project_id == project_id)
        .count()
    )


def create(
    project_id: str,
    name: str,
    description: Optional[str] = None,
    with_commit: bool = False,
) -> KnowledgeBase:
    knowledge_base: KnowledgeBase = KnowledgeBase(project_id=project_id, name=name)
    if description:
        knowledge_base.description = description
    general.add(knowledge_base, with_commit)
    return knowledge_base


def delete(project_id: str, base_id: str, with_commit: bool = False) -> None:
    session.query(KnowledgeBase).filter(
        KnowledgeBase.project_id == project_id, KnowledgeBase.id == base_id
    ).delete()
    general.flush_or_commit(with_commit)


def update(
    project_id: str,
    base_id: str,
    name: str,
    description: str,
    with_commit: bool = False,
) -> KnowledgeBase:
    knowledge_base: KnowledgeBase = get(project_id, base_id)

    if not knowledge_base:
        raise EntityNotFoundException

    if name != knowledge_base.name:
        if not name:
            pass
        elif get_by_name(project_id, name):
            raise EntityAlreadyExistsException
        else:
            knowledge_base.name = name

    if description is not None:
        knowledge_base.description = description
    general.flush_or_commit(with_commit)
    return knowledge_base
