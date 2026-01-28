from datetime import datetime
from . import general, organization, team_member
from .. import User, enums, Team, TeamMember, TeamResource
from ..session import session
from typing import List, Optional
from sqlalchemy import sql

from ..db_cache import TTLCacheDecorator, CacheEnum

from ..util import prevent_sql_injection


def get(user_id: str) -> User:
    return session.query(User).get(user_id)


def get_user_cached_if_not_admin(user_id: str) -> Optional[User]:
    user = get_user_cached(user_id)
    if not user:
        # cache is None, but user is automatically created so we recollect to be sure
        return get(user_id)
    if (user.email or "").endswith("@kern.ai") and user.verified:
        # for admins this could result in two db requests shortly after each other
        # but it's better than having the jumping users without the correct org id
        return get(user_id)
    return user


def get_admin_users() -> List[User]:
    kernai_admins = (
        session.query(User)
        .filter(User.email.ilike("%@kern.ai"), User.verified == True)
        .all()
    )

    query = """
    SELECT email FROM global.full_admin_access
    """

    result = general.execute_all(query)
    full_admin_emails = [row[0].lower() for row in result] if result else []

    if full_admin_emails:
        full_admins = (
            session.query(User).filter(User.email.in_(full_admin_emails)).all()
        )
    else:
        full_admins = []

    admin_users = {user.id: user for user in kernai_admins + full_admins}
    return list(admin_users.values())


def get_engineer_users(org_id: str) -> List[User]:
    engineers = (
        session.query(User)
        .filter(User.role == enums.UserRoles.ENGINEER.value)
        .filter(User.organization_id == org_id)
        .all()
    )
    return engineers


@TTLCacheDecorator(CacheEnum.USER, 5, "user_id")
def get_user_cached(user_id: str) -> User:
    user = get(user_id)
    if not user:
        return None

    general.expunge(user)
    general.make_transient(user)
    return user


def get_by_id_list(user_ids: List[str]) -> List[User]:
    return session.query(User).filter(User.id.in_(user_ids)).all()


def get_all(
    organization_id: Optional[str] = None, user_role: Optional[enums.UserRoles] = None
) -> List[User]:
    query = session.query(User)
    if organization_id:
        query = query.filter(User.organization_id == organization_id)
    if user_role:
        query = query.filter(User.role == user_role.value)
    return query.all()


def get_all_users_by_users_team(user_id: str) -> List[User]:
    if not user_id:
        return []
    teams_subquery = (
        session.query(TeamMember.team_id)
        .filter(TeamMember.user_id == user_id)
        .subquery()
    )
    query = (
        session.query(User)
        .join(TeamMember, TeamMember.user_id == User.id)
        .filter(TeamMember.team_id.in_(sql.select(teams_subquery)))
        .distinct(User.id)
    )
    return query.all()


def get_all_team_members_by_project(project_id: str) -> List[User]:
    query = (
        session.query(TeamMember)
        .join(Team, Team.id == TeamMember.team_id)
        .join(TeamResource, TeamResource.team_id == Team.id)
        .filter(TeamResource.resource_id == project_id)
        .filter(
            TeamResource.resource_type == enums.TeamResourceType.COGNITION_PROJECT.value
        )
    )
    return query.all()


def get_count_assigned() -> int:
    return session.query(User.id).filter(User.organization_id != None).count()


def get_migration_user() -> str:
    query = """
    SELECT u.id
    FROM public.user u
    INNER JOIN organization o
        ON u.organization_id = o.id 
    WHERE o.name = 'migration' 
    """
    u_id = general.execute_first(query)
    if not u_id:
        return __create_migration_user()
    return u_id.id


def create(
    user_id: str, role: Optional[enums.UserRoles] = None, with_commit: bool = False
) -> User:
    """
    This only creates an user in the database but not in the authentication service which is currently kratos.
    The function is e.g. used for project import to be able
    to insert rlas with uid otherwise a foreignkey constraint is hurt.
    These created users can't be resolved the usual way (or at all)
    """
    user = User(id=user_id)
    if role:
        user.role = role.value
    general.add(user, with_commit)
    return user


def remove_organization(user_id: str, with_commit: bool = False) -> None:
    team_member.delete_by_user_id(user_id, with_commit=False)
    user = get(user_id)
    user.organization_id = None
    general.flush_or_commit(with_commit)


def update_organization(
    user_id: str, organization_id: str, with_commit: bool = False
) -> None:
    team_member.delete_by_user_id(user_id, with_commit=False)
    user = get(user_id)
    user.organization_id = organization_id
    general.flush_or_commit(with_commit)


def __create_migration_user() -> str:
    organization_item = organization.get_by_name("migration")
    if not organization_item:
        organization.create("migration")
        return __create_migration_user()
    orga_id = str(organization_item.id)
    query = f"""
    INSERT INTO public.user 
    VALUES ({general.generate_UUID_sql_string()},'{orga_id}');        
    """
    general.execute(query)
    query = f"""
    SELECT id
    FROM public.user
    WHERE organization_id = '{orga_id}'
    """
    user = general.execute_first(query)
    return user.id


def __create_migration_organization():
    query = f"""    
    INSERT INTO organization 
    VALUES ({general.generate_UUID_sql_string()},'migration');
    """
    general.execute(query)


def delete(user_id: str, with_commit: bool = False) -> None:
    session.query(User).filter(User.id == user_id).delete()
    general.flush_or_commit(with_commit)


def get_missing_users(user_ids: List[str]):
    query = f"""
    SELECT jsonb_object_agg(u.id, jsonb_build_object('last_interaction', u.last_interaction,'messages_created_this_month', u.messages_created_this_month, 'messages_created_today', u.messages_created_today))
    FROM public.user u
    WHERE id IN ({",".join([f"'{user_id}'" for user_id in user_ids])})
    """
    value = general.execute_first(query)
    if value is None or value[0] is None:
        return {}
    return value[0]


def get_user_to_organization():
    query = """
    SELECT jsonb_object_agg(u.id, jsonb_build_object('id', o.id, 'name', o.name))
    FROM public.user u
    INNER JOIN organization o
        ON u.organization_id = o.id
    """
    value = general.execute_first(query)
    if value is None or value[0] is None:
        return {}
    return value[0]


def get_active_users_after_filter(
    last_interaction_range: Optional[datetime] = None,
    sort_key: Optional[str] = None,
    sort_direction: Optional[str] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[User]:
    last_interaction_range = prevent_sql_injection(
        last_interaction_range, isinstance(last_interaction_range, datetime)
    )
    sort_key = prevent_sql_injection(sort_key, isinstance(sort_key, str))
    sort_direction = prevent_sql_injection(
        sort_direction, isinstance(sort_direction, str)
    )
    offset = prevent_sql_injection(offset, isinstance(offset, int))
    limit = prevent_sql_injection(limit, isinstance(limit, int))

    query = """
    SELECT u.*, o.name as organization_name
    FROM public.user u 
    LEFT JOIN organization o
        ON u.organization_id = o.id
    WHERE u.email IS NOT NULL
    """

    if last_interaction_range:
        query += f"\nAND last_interaction >= '{last_interaction_range}'"
    if sort_key:
        if sort_key == "organization":
            sort_key = "organization_name"
        sort_direction = "DESC" if sort_direction == -1 else "ASC"
        query += f"\nORDER BY {sort_key} {sort_direction}"
    if offset:
        query += f"\nOFFSET {offset}"
    if limit:
        query += f"\nLIMIT {limit}"

    return general.execute_all(query)


def update_last_interaction(user_id: str) -> None:
    user_item = get(user_id)
    user_item.last_interaction = sql.func.now()
    general.commit()


def check_email_in_full_admin(email: str) -> bool:
    email = email.lower()
    email = prevent_sql_injection(email, isinstance(email, str))
    query = f"""
    SELECT EXISTS (
        SELECT 1
        FROM global.full_admin_access
        WHERE email = '{email}'
    )
    """
    result = general.execute_first(query)
    if result and result[0]:
        return True
    return False
