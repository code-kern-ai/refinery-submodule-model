from ..business_objects import general
from ..session import session
from ..models import GraphRAGIndex
from typing import Dict, Any, List, Tuple
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


def get_all_indexes(org_id: str) -> List[GraphRAGIndex]:
    return (
        session.query(GraphRAGIndex)
        .filter_by(organization_id=org_id)
        .order_by(GraphRAGIndex.created_at)
        .all()
    )


def get_all_indexes_count(org_id: str) -> int:
    return session.query(GraphRAGIndex).filter_by(organization_id=org_id).count()


def update_index_state_error(
    org_id: str, index_id: str, state: str, error: str = None
) -> GraphRAGIndex:
    index = (
        session.query(GraphRAGIndex)
        .filter_by(organization_id=org_id, id=index_id)
        .first()
    )
    index.state = state
    index.error = error
    general.commit()
