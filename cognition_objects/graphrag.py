from ..business_objects import general
from ..session import session
from ..models import GraphRAGIndex
from typing import Dict, Any, List, Tuple


def get_all_indexes(org_id: str) -> List[GraphRAGIndex]:
    return (
        session.query(GraphRAGIndex)
        .filter_by(organization_id=org_id)
        .order_by(GraphRAGIndex.created_at)
        .all()
    )
