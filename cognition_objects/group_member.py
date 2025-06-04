from datetime import datetime
from typing import Optional
from ..business_objects import general, user
from . import group
from ..session import session
from ..models import CognitionGroupMember


def get(group_id: str, id: str):
    return (
        session.query(CognitionGroupMember)
        .filter(CognitionGroupMember.group_id == group_id, CognitionGroupMember.id == id)
        .first()
    )


def get_by_group_and_user(group_id: str, user_id: str) -> CognitionGroupMember:
    return (
        session.query(CognitionGroupMember)
        .filter(CognitionGroupMember.group_id == group_id, CognitionGroupMember.user_id == user_id)
        .first()
    )


def get_by_user_id(user_id: str) -> list:
    return session.query(CognitionGroupMember).filter(CognitionGroupMember.user_id == user_id).all()


def get_all_by_group(group_id: str) -> list:
    return session.query(CognitionGroupMember).filter(CognitionGroupMember.group_id == group_id).all()


def get_all_by_group_count(group_id: str) -> int:
    return session.query(CognitionGroupMember).filter(CognitionGroupMember.group_id == group_id).count()


def create(
    group_id: str,
    user_id: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> CognitionGroupMember:
    already_exist = get_by_group_and_user(group_id=group_id, user_id=user_id)
    if already_exist:
        return already_exist

    group_item = group.get(group_id)
    user_item = user.get(user_id)
    if not group_item or not user_item:
        raise Exception("Group or user not found")
    if group_item.organization_id != user_item.organization_id:
        raise Exception("User not in the same organization as the group")

    group_member = CognitionGroupMember(
        group_id=group_id,
        user_id=user_id,
        created_at=created_at,
    )
    general.add(group_member, with_commit)
    return group_member


def delete_by_group_and_user_id(
    group_id: str, user_id: str, with_commit: bool = False
) -> None:
    group_member = get_by_group_and_user(group_id, user_id)
    if group_member:
        general.delete(group_member, with_commit)


def delete_by_user_id(user_id: str, with_commit: bool = False) -> None:
    group_memberships = (
        session.query(CognitionGroupMember).filter(CognitionGroupMember.user_id == user_id).all()
    )
    for membership in group_memberships:
        general.delete(membership, with_commit=False)
    general.flush_or_commit(with_commit)
