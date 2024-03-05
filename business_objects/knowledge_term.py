from typing import Iterable, List, Any, Optional

from . import general
from ..models import KnowledgeTerm, KnowledgeBase
from ..exceptions import (
    EntityAlreadyExistsException,
    EntityNotFoundException,
)
from ..session import session
from ..business_objects import knowledge_base
from ..util import prevent_sql_injection


def get_by_value(knowledge_base_id: str, value: str) -> KnowledgeTerm:
    return (
        session.query(KnowledgeTerm)
        .filter(
            KnowledgeTerm.knowledge_base_id == knowledge_base_id,
            KnowledgeTerm.value == value,
        )
        .first()
    )


def get_by_id(term_id: str) -> KnowledgeTerm:
    return (
        session.query(KnowledgeTerm)
        .filter(
            KnowledgeTerm.id == term_id,
        )
        .first()
    )


def get_by_knowledge_base(knowledge_base_id: str) -> List[KnowledgeTerm]:
    return (
        session.query(KnowledgeTerm)
        .filter(KnowledgeTerm.knowledge_base_id == knowledge_base_id)
        .all()
    )


def get_terms_with_base_names(
    project_id: str, only_whitelisted: bool = True
) -> List[Any]:
    query = session.query(KnowledgeTerm.value, KnowledgeBase.name).filter(
        KnowledgeTerm.knowledge_base_id == KnowledgeBase.id,
        KnowledgeBase.project_id == project_id,
    )
    if only_whitelisted:
        query = query.filter(KnowledgeTerm.blacklisted == False)
    return query.all()


def get_terms_by_project_id(project_id: str) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
        SELECT
            knowledge_base.id,
            knowledge_term.value,
            knowledge_term.comment,
            knowledge_term.blacklisted
        FROM
            knowledge_base
        INNER JOIN
            knowledge_term
        ON
            knowledge_base.id=knowledge_term.knowledge_base_id
        WHERE
            knowledge_base.project_id='{project_id}'
        ;
        """
    return general.execute_all(query)


def count(project_id: str, knowledge_base_id: str) -> int:
    return (
        session.query(KnowledgeTerm)
        .join(KnowledgeBase, (KnowledgeTerm.knowledge_base_id == KnowledgeBase.id))
        .filter(
            KnowledgeBase.project_id == project_id,
            KnowledgeBase.id == knowledge_base_id,
        )
        .count()
    )


def create(
    project_id: str,
    knowledge_base_id: str,
    value: str,
    comment: str,
    blacklisted: Optional[bool] = None,
    with_commit: bool = False,
) -> KnowledgeTerm:
    base: KnowledgeBase = knowledge_base.get(project_id, knowledge_base_id)
    if not base:
        raise EntityNotFoundException

    term: KnowledgeTerm = get_by_value(knowledge_base_id, value)
    if term:
        raise EntityAlreadyExistsException
    term: KnowledgeTerm = KnowledgeTerm(
        project_id=project_id,
        value=value,
        comment=comment,
        knowledge_base_id=knowledge_base_id,
    )
    if blacklisted:
        term.blacklisted = blacklisted
    general.add(term, with_commit)
    return term


def create_by_value_list(
    project_id: str,
    knowledge_base_id: str,
    values: Iterable[str],
    with_commit: bool = False,
) -> List[KnowledgeTerm]:
    base: KnowledgeBase = knowledge_base.get(project_id, knowledge_base_id)
    if not base:
        raise EntityNotFoundException
    terms = [
        KnowledgeTerm(
            project_id=project_id,
            knowledge_base_id=knowledge_base_id,
            value=value,
            comment="",
        )
        for value in values
    ]
    general.add_all(terms, with_commit=with_commit)

    return terms


def update(
    knowledge_base_id: str,
    term_id: str,
    value: str,
    comment: str,
    with_commit: bool = False,
) -> KnowledgeTerm:
    term: KnowledgeTerm = get_by_id(term_id)
    if not term:
        raise EntityNotFoundException

    if term.value != value:
        if get_by_value(knowledge_base_id, value):
            raise EntityAlreadyExistsException

    term.value = value
    term.comment = comment
    general.flush_or_commit(with_commit)
    return term


def blacklist(term_id: str, with_commit: bool = False) -> None:
    term: KnowledgeTerm = get_by_id(term_id)
    term.blacklisted = not term.blacklisted
    general.flush_or_commit(with_commit)


def delete(term_id: str, with_commit: bool = False) -> None:
    session.query(KnowledgeTerm).filter(KnowledgeTerm.id == term_id).delete()
    general.flush_or_commit(with_commit)


def delete_by_value_list(
    knowledge_base_id: str, value_list: Iterable[str], with_commit: bool = False
) -> None:
    session.query(KnowledgeTerm).filter(
        KnowledgeTerm.knowledge_base_id == knowledge_base_id,
        KnowledgeTerm.value.in_(value_list),
    ).delete()
    general.flush_or_commit(with_commit)
