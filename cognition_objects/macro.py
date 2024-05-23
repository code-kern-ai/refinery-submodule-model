from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownDataset, CognitionMacro, User, CognitionProject
from ..enums import (
    Tablenames,
    MarkdownFileCategoryOrigin,
    AdminMacrosDisplay,
    UserRoles,
    MacroScope,
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
        final_list.extend(__get_project_macros_for_me(user, project_item))
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


def __get_project_macros_for_me(
    user: User, project: CognitionProject
) -> List[CognitionMacro]:
    return (
        session.query(CognitionMacro)
        .filter(
            CognitionMacro.scope == MacroScope.PROJECT.value,
            CognitionMacro.project_id == project.id,
        )
        .all()
    )
    return (
        session.query(CognitionMacro)
        .filter(
            CognitionMacro.scope == MacroScope.ORGANIZATION.value,
            CognitionMacro.organization_id == user.organization_id,
        )
        .all()
    )


# export type MacroOverview = {
#     id: string;
#     name: string;
#     macroType: MacroType;
#     description: string;
#     groupKey: string;
#     scope: "ADMIN" | "ORGANIZATION" | "PROJECT";
#     state: "DEVELOPMENT" | "PRODUCTION";
#     projectId: string; // null for admin/org macros
# }
