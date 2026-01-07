from typing import List

from submodules.model.business_objects import general
from submodules.model.session import session
from submodules.model import RefineryKnowledgeGraph
from submodules.model.enums import KnowledgeGraphType


def get(org_id: str, id: str) -> RefineryKnowledgeGraph:
    return (
        session.query(RefineryKnowledgeGraph)
        .filter(
            RefineryKnowledgeGraph.organization_id == org_id,
            RefineryKnowledgeGraph.id == id,
        )
        .first()
    )


def get_by_id(id: str) -> RefineryKnowledgeGraph:
    return (
        session.query(RefineryKnowledgeGraph)
        .filter(RefineryKnowledgeGraph.id == id)
        .first()
    )


def get_by_project_id(org_id: str, project_id: str) -> List[RefineryKnowledgeGraph]:
    return (
        session.query(RefineryKnowledgeGraph)
        .filter(
            RefineryKnowledgeGraph.organization_id == org_id,
            RefineryKnowledgeGraph.project_id == project_id,
        )
        .all()
    )


def get_by_project_id_and_type(
    org_id: str, project_id: str, type: KnowledgeGraphType
) -> RefineryKnowledgeGraph:
    return (
        session.query(RefineryKnowledgeGraph)
        .filter(
            RefineryKnowledgeGraph.organization_id == org_id,
            RefineryKnowledgeGraph.project_id == project_id,
            RefineryKnowledgeGraph.type == type.value,
        )
        .first()
    )


def create(
    org_id: str,
    user_id: str,
    project_id: str,
    name: str,
    description: str,
    type: KnowledgeGraphType,
    with_commit: bool = True,
) -> RefineryKnowledgeGraph:
    knowledge_graph = RefineryKnowledgeGraph(
        organization_id=org_id,
        created_by=user_id,
        project_id=project_id,
        name=name,
        description=description,
        type=type.value,
    )
    general.add(knowledge_graph, with_commit)
    return knowledge_graph


def update(
    org_id: str,
    knowledge_graph_id: str,
    name: str,
    description: str,
    with_commit: bool = True,
) -> RefineryKnowledgeGraph:
    knowledge_graph = get(org_id, knowledge_graph_id)

    if name:
        knowledge_graph.name = name
    if description:
        knowledge_graph.description = description
    general.add(knowledge_graph, with_commit)
    return knowledge_graph


def delete_many(
    org_id: str, project_id: str, ids: List[str], with_commit: bool = False
) -> None:
    session.query(RefineryKnowledgeGraph).filter(
        RefineryKnowledgeGraph.organization_id == org_id,
        RefineryKnowledgeGraph.project_id == project_id,
        RefineryKnowledgeGraph.id.in_(ids),
    ).delete()
    general.flush_or_commit(with_commit)
