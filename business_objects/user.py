from . import general, organization, team_member
from .. import User, enums
from ..session import session
from typing import List, Any, Optional

from ..util import prevent_sql_injection


def get(user_id: str) -> User:
    return session.query(User).get(user_id)


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
