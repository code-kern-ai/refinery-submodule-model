from ..business_objects import general
from ..session import session
from ..models import GraphRAGIndex
from typing import Dict, Any, List, Optional, Iterable
from submodules.model.enums import GraphRAGIndexState


def create(org_id: str, name: str, description: str, user_id: str) -> GraphRAGIndex:
    graphrag_index = GraphRAGIndex(
        organization_id=org_id,
        name=name,
        description=description,
        state=GraphRAGIndexState.CREATED.value,
        created_by=user_id,
    )
    general.add(graphrag_index, with_commit=True)
    return graphrag_index


def get(org_id: str, index_id: str) -> GraphRAGIndex:
    return (
        session.query(GraphRAGIndex)
        .filter_by(organization_id=org_id, id=index_id)
        .first()
    )


def get_all_indexes(org_id: str, include_failed=True) -> List[GraphRAGIndex]:
    if include_failed:
        return (
            session.query(GraphRAGIndex)
            .filter_by(organization_id=org_id)
            .order_by(GraphRAGIndex.created_at)
            .all()
        )
    return (
        session.query(GraphRAGIndex)
        .filter(GraphRAGIndex.organization_id == org_id)
        .filter(GraphRAGIndex.state != GraphRAGIndexState.FAILED.value)
        .order_by(GraphRAGIndex.created_at)
        .all()
    )


def get_all_indexes_count(org_id: str, include_failed=True) -> int:
    if include_failed:
        return session.query(GraphRAGIndex).filter_by(organization_id=org_id).count()
    return (
        session.query(GraphRAGIndex)
        .filter(GraphRAGIndex.organization_id == org_id)
        .filter(GraphRAGIndex.state != GraphRAGIndexState.FAILED.value)
        .count()
    )


def get_all_paginated_by_org_id(org_id: str, page: int, limit: int) -> Dict[str, Any]:
    total_count = (
        session.query(GraphRAGIndex.id)
        .filter(GraphRAGIndex.organization_id == org_id)
        .count()
    )

    num_pages = int(total_count / limit)
    if total_count % limit > 0:
        num_pages += 1
    if page > 0:
        paginated_result = (
            session.query(GraphRAGIndex)
            .filter(GraphRAGIndex.organization_id == org_id)
            .order_by(GraphRAGIndex.created_at.desc())
            .limit(limit)
            .offset((page - 1) * limit)
            .all()
        )
    else:
        paginated_result = []
    return total_count, num_pages, paginated_result


def update(
    org_id: str,
    index_id: str,
    state: Optional[str] = None,
    error: Optional[str] = None,
    root_dir: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    with_commit: Optional[bool] = True,
) -> GraphRAGIndex:
    index = (
        session.query(GraphRAGIndex)
        .filter_by(organization_id=org_id, id=index_id)
        .first()
    )
    if state is not None:
        index.state = state
    if error is not None:
        index.error = error
    if root_dir is not None:
        index.root_dir = root_dir
    if settings is not None:
        index.settings = settings
    general.flush_or_commit(with_commit)
    return index


def delete_many(
    org_id: str,
    index_ids: Iterable[str],
    with_commit: Optional[bool] = True,
) -> None:

    session.query(GraphRAGIndex).filter(
        GraphRAGIndex.organization_id == org_id,
        GraphRAGIndex.id.in_(index_ids),
    ).delete(synchronize_session=False)

    general.flush_or_commit(with_commit)
