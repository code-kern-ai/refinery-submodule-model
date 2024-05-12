from typing import List, Optional, Dict, Any
from ..business_objects import general
from ..cognition_objects import project
from ..session import session
from ..models import CognitionAction, CognitionQuestionNode, CognitionQuestionEdge
from datetime import datetime
from sqlalchemy import or_

def get_all_by_project_id(
    project_id: str,
) -> List[CognitionAction]:
    return (
        session.query(CognitionAction)
        .filter(
            CognitionAction.project_id == project_id,
            )
        .order_by(CognitionAction.created_at.asc())
        .all()
    )

def get(
    project_id: str,
    action_id: str,
) -> CognitionAction:
    return (
        session.query(CognitionAction)
        .filter(
            CognitionAction.project_id == project_id,
            CognitionAction.id == action_id,
        )
        .first()
    )


def get_nodes_for_action(
    project_id: str,
    action_id: str,
) -> List[CognitionQuestionNode]:
    return (
        session.query(CognitionQuestionNode)
        .filter(
            CognitionQuestionNode.action_id == action_id,
            CognitionQuestionNode.project_id == project_id,
        )
        .order_by(CognitionQuestionNode.created_at.asc())
        .all()
    )

def get_node_by_id(
    project_id: str,
    node_id: str,
) -> CognitionQuestionNode:
    return (
        session.query(CognitionQuestionNode)
        .filter(
            CognitionQuestionNode.id == node_id,
            CognitionQuestionNode.project_id == project_id,
        )
        .first()
    )

def get_edges_for_action(
    project_id: str,
    action_id: str,
) -> List[CognitionQuestionEdge]:
    return (
        session.query(CognitionQuestionEdge)
        .filter(
            CognitionQuestionEdge.action_id == action_id,
            CognitionQuestionEdge.project_id == project_id,
        )
        .order_by(CognitionQuestionEdge.created_at.asc())
        .all()
    )

def get_edges_leading_to_nodes(
    project_id: str,
    action_id: str,
    node_ids: List[str],
) -> List[CognitionQuestionEdge]:
    return (
        session.query(CognitionQuestionEdge)
        .filter(
            CognitionQuestionEdge.action_id == action_id,
            CognitionQuestionEdge.project_id == project_id,
            CognitionQuestionEdge.to_node_id.in_(node_ids),
        )
        .order_by(CognitionQuestionEdge.created_at.asc())
        .all()
    )

def get_root_node_of_action(
    project_id: str,
    action_id: str,
) -> CognitionQuestionNode:
    return (
        session.query(CognitionQuestionNode)
        .filter(
            CognitionQuestionNode.action_id == action_id,
            CognitionQuestionNode.project_id == project_id,
            CognitionQuestionNode.root == True,
        )
        .first()
    )

def get_children_of_node(
    project_id: str,
    action_id: str,
    node_id: str,
) -> List[CognitionQuestionNode]:
    # Fetch the IDs of nodes that are children of the specified node_id
    child_node_ids = (
        session.query(CognitionQuestionEdge.to_node_id)
        .filter(
            CognitionQuestionEdge.action_id == action_id,
            CognitionQuestionEdge.project_id == project_id,
            CognitionQuestionEdge.from_node_id == node_id,
        )
        .all()
    )
    # Convert list of tuples to list of ids
    child_node_ids = [id[0] for id in child_node_ids]

    # Fetch the node entities for these child IDs
    if child_node_ids:
        return (
            session.query(CognitionQuestionNode)
            .filter(
                CognitionQuestionNode.id.in_(child_node_ids),
                CognitionQuestionNode.action_id == action_id,
                CognitionQuestionNode.project_id == project_id,
            )
            .order_by(CognitionQuestionNode.created_at.asc())
            .all()
        )
    else:
        return []

def create(
    project_id: str,
    user_id: str,
    name: str,
    description: str,
    nodes: List[Dict[str, Any]],
    edges: List[Dict[str, Any]],
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionAction:
    
    project_entity = project.get(project_id)

    action = CognitionAction(
        project_id=project_entity.id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
    )
    general.add(action, with_commit)
    add_nodes_and_edges(action, project_entity, user_id, nodes, edges, with_commit)
    return action

def add_nodes_and_edges(action, project_entity, user_id, nodes, edges, with_commit):
    nodes_entity_node_arg_pair_list = []
    id_mapping_list = {}
    for node in nodes:
        node_entity = CognitionQuestionNode(
            action_id=action.id,
            name=node.name,
            question=node.question,
            position_x=node.positionX,
            position_y=node.positionY,
            root=node.root,
            project_id=project_entity.id,
            created_by=user_id,
        )
        nodes_entity_node_arg_pair_list.append([node_entity, node])
    general.add_all([node_entity for node_entity, node_arg in nodes_entity_node_arg_pair_list], with_commit)

    for node_entity, node_arg in nodes_entity_node_arg_pair_list:
        id_mapping_list[node_arg.id] = node_entity.id
    edges_entity_list = []
    for edge in edges:
        if edge.from_node_id not in id_mapping_list or edge.to_node_id not in id_mapping_list:
            continue # Skip edges that have nodes that were not created
        edge_entity = CognitionQuestionEdge(
            action_id=action.id,
            from_node_id=id_mapping_list[edge.from_node_id],
            to_node_id=id_mapping_list[edge.to_node_id],
            condition=edge.condition,
            project_id=project_entity.id,
        )
        edges_entity_list.append(edge_entity)
    general.add_all(edges_entity_list, with_commit)


def update(
    project_id: str,
    action_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    nodes: Optional[List[Dict[str, Any]]] = None,
    edges: Optional[List[Dict[str, Any]]] = None,
    with_commit: bool = True,
) -> CognitionAction:
    action = get(project_id, action_id)

    if name is not None:
        action.name = name
    if description is not None:
        action.description = description

    if nodes is not None and edges is not None:
        delete_nodes_of_action(project_id, action_id, with_commit)
        delete_edges_of_action(project_id, action_id, with_commit)
        add_nodes_and_edges(action, project.get(project_id), action.created_by, nodes, edges, with_commit)


    general.add(action, with_commit)
    return action


def delete_by_ids(
    project_id: str,
    ids: List[str],
    with_commit: bool = True,
) -> None:
    session.query(CognitionQuestionNode).filter(
        CognitionQuestionNode.action_id.in_(ids),
    ).delete()

    session.query(CognitionQuestionEdge).filter(
        CognitionQuestionEdge.action_id.in_(ids),
    ).delete()

    session.query(CognitionAction).filter(
        CognitionAction.project_id == project_id,
        CognitionAction.id.in_(ids),
    ).delete()

    general.flush_or_commit(with_commit)

def delete_nodes_of_action(
    project_id: str,
    action_id: str,
    with_commit: bool = True,
) -> None:
    session.query(CognitionQuestionNode).filter(
        CognitionQuestionNode.action_id == action_id,
        CognitionQuestionNode.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)

def delete_node(   
    project_id: str,
    action_id: str,
    node_id: str,
    with_commit: bool = True,
) -> None:
    
    delete_edges_of_node(project_id, action_id, node_id, with_commit)

    session.query(CognitionQuestionNode).filter(
        CognitionQuestionNode.id == node_id,
        CognitionQuestionNode.action_id == action_id,
        CognitionQuestionNode.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)

def delete_edges_of_action(
    project_id: str,
    action_id: str,
    with_commit: bool = True,
) -> None:
    session.query(CognitionQuestionEdge).filter(
        CognitionQuestionEdge.action_id == action_id,
        CognitionQuestionEdge.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)

def delete_edges_of_node(
    project_id: str,
    action_id: str,
    node_id: str,
    with_commit: bool = True,
) -> None:
    session.query(CognitionQuestionEdge).filter(
        CognitionQuestionEdge.action_id == action_id,
        CognitionQuestionEdge.project_id == project_id,
        or_(CognitionQuestionEdge.from_node_id == node_id, CognitionQuestionEdge.to_node_id == node_id),
    ).delete()
    general.flush_or_commit(with_commit)

def delete_edge(
    project_id: str,
    action_id: str,
    edge_id: str,
    with_commit: bool = True,
) -> None:
    session.query(CognitionQuestionEdge).filter(
        CognitionQuestionEdge.id == edge_id,
        CognitionQuestionEdge.action_id == action_id,
        CognitionQuestionEdge.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit
)