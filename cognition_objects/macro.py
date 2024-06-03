from typing import List, Optional, Tuple, Dict, Any, Iterable, Union
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import (
    CognitionMacroNode,
    CognitionMacroEdge,
    CognitionMacro,
    User,
    CognitionProject,
    CognitionMacroExecution,
)
from ..enums import (
    Tablenames,
    MarkdownFileCategoryOrigin,
    AdminMacrosDisplay,
    UserRoles,
    MacroScope,
    MacroType,
    MacroState,
    MacroExecutionState,
)
from ..util import prevent_sql_injection, is_list_like
from . import project
from sqlalchemy import or_, and_
from sqlalchemy.orm.attributes import flag_modified


def get(id: str) -> CognitionMacro:
    return (
        session.query(CognitionMacro)
        .filter(
            CognitionMacro.id == id,
        )
        .first()
    )


def get_with_nodes_and_edges(macro_id: str) -> Dict[str, Any]:
    macro_id = prevent_sql_injection(macro_id, isinstance(macro_id, str))
    query = f"""
    SELECT row_to_json(x)
    FROM (
        SELECT M.*, me.edges, mn.nodes
        FROM cognition.macro M,
        (
            SELECT array_agg(row_to_json(me.*)) edges
            FROM cognition.macro_edge me
            WHERE me.macro_id = '{macro_id}'
        ) me,
        (
            SELECT array_agg(row_to_json(mn.*)) nodes
            FROM cognition.macro_node mn
            WHERE mn.macro_id = '{macro_id}'
        ) mn
        WHERE m.id = '{macro_id}' )x """
    result = general.execute_first(query)
    if result and result[0]:
        return result[0]
    return None


def get_overview_for_all_for_me(
    user: User,
    is_admin: bool,
    project_id: Optional[str] = None,
    only_production: bool = False,
) -> List[CognitionMacro]:
    project_item = project.get(project_id) if project_id else None
    final_list = []
    final_list = __get_admin_macros_for_me(
        user, is_admin, project_item, only_production
    )
    final_list.extend(__get_org_macros_for_me(user, only_production))
    if project_id:
        final_list.extend(__get_project_macros_for_me(project_item, only_production))
    return final_list


def get_macro_execution(
    execution_id: str,
    execution_group_id: str,
    state: Optional[Union[MacroExecutionState, List[MacroExecutionState]]] = None,
) -> CognitionMacroExecution:

    query = session.query(CognitionMacroExecution).filter(
        CognitionMacroExecution.id == execution_id,
        CognitionMacroExecution.execution_group_id == execution_group_id,
    )
    if state:
        if is_list_like(state):
            query = query.filter(
                CognitionMacroExecution.state.in_([s.value for s in state])
            )
        else:
            query = query.filter(CognitionMacroExecution.state == state.value)
    return query.first()


def macro_execution_group_finished(macro_id: str, execution_group_id: str) -> bool:
    return (
        session.query(CognitionMacroExecution)
        .filter(
            CognitionMacroExecution.macro_id == macro_id,
            CognitionMacroExecution.execution_group_id == execution_group_id,
            CognitionMacroExecution.state._in(
                [MacroExecutionState.CREATED.value, MacroExecutionState.RUNNING.value]
            ),
        )
        .first()
        is None
    )


def __get_admin_macros_for_me(
    user: User, is_admin: bool, project: CognitionProject, only_production: bool
) -> List[CognitionMacro]:

    if (
        not project
        or not project.macro_config
        or not (show := project.macro_config.get("show"))
    ):
        return []

    if (
        (show == AdminMacrosDisplay.DONT_SHOW.value)
        or (show == AdminMacrosDisplay.FOR_ADMINS.value and not is_admin)
        or (
            show == AdminMacrosDisplay.FOR_ENGINEERS.value
            and user.role != UserRoles.ENGINEER.value
        )
    ):
        return []
    query = session.query(CognitionMacro).filter(
        CognitionMacro.scope == MacroScope.ADMIN.value
    )

    if only_production:
        query = query.filter(CognitionMacro.state == MacroState.PRODUCTION.value)

    return query.all()


def __get_org_macros_for_me(user: User, only_production: bool) -> List[CognitionMacro]:
    query = session.query(CognitionMacro).filter(
        CognitionMacro.scope == MacroScope.ORGANIZATION.value,
        CognitionMacro.organization_id == user.organization_id,
    )
    if only_production:
        query = query.filter(CognitionMacro.state == MacroState.PRODUCTION.value)

    return query.all()


def __get_project_macros_for_me(
    project: CognitionProject, only_production: bool
) -> List[CognitionMacro]:
    query = session.query(CognitionMacro).filter(
        CognitionMacro.scope == MacroScope.PROJECT.value,
        CognitionMacro.organization_id == project.organization_id,
        CognitionMacro.project_id == project.id,
    )
    if only_production:
        query = query.filter(CognitionMacro.state == MacroState.PRODUCTION.value)

    return query.all()


def create_macro(
    macro_type: MacroType,
    scope: MacroScope,
    state: MacroState,
    created_by: str,
    name: str,
    description: Optional[str] = None,
    organization_id: Optional[str] = None,
    project_id: Optional[str] = None,
    with_commit: bool = False,  # usually with nodes & edges
) -> CognitionMacro:
    mac = CognitionMacro(
        macro_type=macro_type.value,
        scope=scope.value,
        state=state.value,
        created_by=created_by,
        name=name,
        description=description,
        organization_id=organization_id,
        project_id=project_id,
    )

    general.add(mac, with_commit)

    return mac


def create_node(
    macro_id: str,
    created_by: str,
    is_root: bool,
    config: Dict[str, Any],
    id: Optional[str] = None,  # usually frontend generates the ids during creation
    with_commit: bool = False,
) -> CognitionMacroNode:
    node = CognitionMacroNode(
        id=id,
        macro_id=macro_id,
        created_by=created_by,
        is_root=is_root,
        config=config,
    )

    general.add(node, with_commit)

    return node


def create_edge(
    macro_id: str,
    from_node_id: str,
    to_node_id: str,
    created_by: str,
    config: Dict[str, Any],
    id: Optional[str] = None,  # usually frontend generates the ids during creation
    with_commit: bool = False,
) -> CognitionMacroEdge:
    edge = CognitionMacroEdge(
        id=id,
        macro_id=macro_id,
        from_node_id=from_node_id,
        to_node_id=to_node_id,
        created_by=created_by,
        config=config,
    )

    general.add(edge, with_commit)

    return edge


def delete_macros(
    org_id: str,
    ids: Iterable[str],
    is_admin: bool,
    user: User,
    with_commit: bool = True,
    # returns the ids that couldn't be deleted
) -> List[str]:
    #
    query = session.query(CognitionMacro).filter(
        CognitionMacro.id.in_(ids),
        or_(
            CognitionMacro.organization_id == org_id,
            and_(
                CognitionMacro.scope == MacroScope.ADMIN.value,
                CognitionMacro.organization_id.is_(None),
            ),
        ),
    )
    # filter_org =
    if user.role != UserRoles.ENGINEER.value:
        # can only delete their own macros
        query = query.filter(CognitionMacro.created_by == user.id)
    if not is_admin:
        # can't delete admin macros
        query = query.filter(CognitionMacro.scope != MacroScope.ADMIN.value)
    query.delete()
    general.flush_or_commit(with_commit)

    objs = session.query(CognitionMacro.id).filter(CognitionMacro.id.in_(ids)).all()
    return [str(obj.id) for obj in objs]


# creates, updates, deletes nodes based on the updated_nodes list
def match_nodes(
    macro_id: str, update_nodes: List[Dict[str, Any]], with_commit: bool = False
) -> None:

    current: List[CognitionMacroNode] = (
        session.query(CognitionMacroNode)
        .filter(
            CognitionMacroNode.macro_id == macro_id,
        )
        .all()
    )

    # dict to get values
    wanted = {n["id"]: n for n in update_nodes}
    # set for faster lookup
    found = {str(n.id) for n in current}
    # lists for faster generation
    to_delete = [id for id in found if id not in wanted]
    to_update = [n for n in current if str(n.id) in wanted]
    to_create = [n for n in update_nodes if n["id"] not in found]

    session.query(CognitionMacroNode).filter(
        CognitionMacroNode.id.in_(to_delete),
    ).delete(synchronize_session=False)

    for node in to_update:
        update_values = wanted.get(str(node.id))
        if not update_values:
            raise ValueError(f"Node with id {node.id} not found in update_nodes")
        node.is_root = update_values["is_root"]
        node.config = update_values["config"]
        flag_modified(node, "config")

    for node in to_create:
        create_node(
            macro_id=macro_id,
            created_by=node["created_by"],
            is_root=node["is_root"],
            config=node["config"],
            id=node["id"],
            with_commit=False,
        )

    general.flush_or_commit(with_commit)


# creates, updates, deletes edges based on the update_edges list
def match_edges(
    macro_id: str, update_edges: List[Dict[str, Any]], with_commit: bool = False
) -> None:
    current: List[CognitionMacroEdge] = (
        session.query(CognitionMacroEdge)
        .filter(
            CognitionMacroEdge.macro_id == macro_id,
        )
        .all()
    )
    # dict to get values
    wanted = {e["id"]: e for e in update_edges}
    # set for faster lookup
    found = {str(e.id) for e in current}
    # lists for faster generation
    to_delete = [id for id in found if id not in wanted]
    to_update = [e for e in current if e.id in wanted]
    to_create = [e for e in update_edges if e["id"] not in found]
    for edge in to_update:
        update_values = wanted.get(str(edge.id))
        if not update_values:
            raise ValueError(f"Edge with id {edge.id} not found in update_edges")
        edge.from_node_id = update_values["from_node_id"]
        edge.to_node_id = update_values["to_node_id"]
        edge.config = update_values["config"]
        flag_modified(edge, "config")

    for edge in to_create:
        create_edge(
            macro_id=macro_id,
            from_node_id=edge["from_node_id"],
            to_node_id=edge["to_node_id"],
            created_by=edge["created_by"],
            config=edge["config"],
            id=edge["id"],
            with_commit=False,
        )

    session.query(CognitionMacroEdge).filter(
        CognitionMacroEdge.id.in_(to_delete),
    ).delete(synchronize_session=False)

    general.flush_or_commit(with_commit)


def create_macro_execution(
    macro_id: str,
    created_by: str,
    state: MacroExecutionState,  # CREATED, FINISHED, FAILED
    execution_group_id: str,
    meta_info: Dict[str, Any],
    with_commit: bool = False,
) -> CognitionMacroExecution:
    mac = CognitionMacroExecution(
        macro_id=macro_id,
        created_by=created_by,
        state=state.value,
        execution_group_id=execution_group_id,
        meta_info=meta_info,
    )

    general.add(mac, with_commit)

    return mac
