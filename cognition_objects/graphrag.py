from ..business_objects import general
from ..session import session
from ..models import GraphRAGIndex
from typing import Dict, Any, List, Tuple, Optional
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


def get_all_indexes(org_id: str) -> List[GraphRAGIndex]:
    return (
        session.query(GraphRAGIndex)
        .filter_by(organization_id=org_id)
        .order_by(GraphRAGIndex.created_at)
        .all()
    )


def get_all_indexes_count(org_id: str) -> int:
    return session.query(GraphRAGIndex).filter_by(organization_id=org_id).count()


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
