from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import (
    CognitionMacroNode,
    CognitionMacroEdge,
    CognitionMacro,
    User,
    CognitionProject,
)
from ..enums import (
    Tablenames,
    MarkdownFileCategoryOrigin,
    AdminMacrosDisplay,
    UserRoles,
    MacroScope,
    MacroType,
    MacroState,
)
from ..util import prevent_sql_injection
from . import project


def get(id: str) -> CognitionMacro:
    return (
        session.query(CognitionMacro)
        .filter(
            CognitionMacro.id == id,
        )
        .first()
    )


def get_overview_for_all_for_me(
    user: User, is_admin: bool, project_id: Optional[str] = None
) -> List[CognitionMacro]:
    project_item = project.get(project_id) if project_id else None
    final_list = []
    final_list = __get_admin_macros_for_me(user, is_admin, project_item)
    final_list.extend(__get_org_macros_for_me(user))
    if project_id:
        final_list.extend(__get_project_macros_for_me(project_item))
    return final_list


def __get_admin_macros_for_me(
    user: User, is_admin: bool, project: CognitionProject
) -> List[CognitionMacro]:
    if not project.macro_config or not (show := project.macro_config.get("show")):
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
    return (
        session.query(CognitionMacro)
        .filter(CognitionMacro.scope == MacroScope.ADMIN.value)
        .all()
    )


def __get_org_macros_for_me(user: User) -> List[CognitionMacro]:
    return (
        session.query(CognitionMacro)
        .filter(
            CognitionMacro.scope == MacroScope.ORGANIZATION.value,
            CognitionMacro.organization_id == user.organization_id,
        )
        .all()
    )


def __get_project_macros_for_me(project: CognitionProject) -> List[CognitionMacro]:
    return (
        session.query(CognitionMacro)
        .filter(
            CognitionMacro.scope == MacroScope.PROJECT.value,
            CognitionMacro.organization_id == project.organization_id,
            CognitionMacro.project_id == project.id,
        )
        .all()
    )


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
