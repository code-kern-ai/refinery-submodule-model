from operator import or_
from typing import List, Optional
from ..business_objects import general
from ..session import session
from ..models import ConversationShare, CognitionConversation
from submodules.model.util import sql_alchemy_to_dict


def get(share_id: str, user_id: str) -> Optional[ConversationShare]:
    return (
        session.query(ConversationShare)
        .filter(ConversationShare.id == share_id)
        .filter(
            or_(
                ConversationShare.shared_by == user_id,
                ConversationShare.shared_with == user_id,
            )
        )
        .first()
    )


def get_all_by_conversation(conversation_id: str) -> List[ConversationShare]:
    return (
        session.query(ConversationShare)
        .filter(ConversationShare.conversation_id == conversation_id)
        .all()
    )


def update_by_conversation(
    conversation_id: str,
    user_id: str,
    shared_with: List[str],
    can_copy: Optional[bool] = None,
    with_commit: bool = True,
) -> List[ConversationShare]:
    existing_shares = (
        session.query(ConversationShare)
        .filter(ConversationShare.conversation_id == conversation_id)
        .filter(ConversationShare.shared_by == user_id)
        .all()
    )

    existing_shared_with = {share.shared_with: share for share in existing_shares}
    shared_with_set = set(shared_with)

    for share in existing_shares:
        if share.shared_with not in shared_with_set:
            session.delete(share)

    for sharing_user_id in shared_with:
        if sharing_user_id not in existing_shared_with:
            share = ConversationShare(
                conversation_id=conversation_id,
                shared_with=sharing_user_id,
                shared_by=user_id,
                can_copy=can_copy if can_copy is not None else False,
            )
            general.add(share, with_commit=False)
        else:
            if can_copy is not None:
                existing_shared_with[sharing_user_id].can_copy = can_copy

    general.flush_or_commit(with_commit)

    updated_shares = (
        session.query(ConversationShare)
        .filter(ConversationShare.conversation_id == conversation_id)
        .filter(ConversationShare.shared_by == user_id)
        .all()
    )
    return updated_shares


def get_all_shared_by_or_for_user(
    project_id: str, user_id: str, with_header: bool = True
) -> List[ConversationShare]:
    conversation_shares = (
        session.query(ConversationShare, CognitionConversation.header)
        .join(
            CognitionConversation,
            ConversationShare.conversation_id == CognitionConversation.id,
        )
        .filter(CognitionConversation.project_id == project_id)
        .filter(
            or_(
                ConversationShare.shared_by == user_id,
                ConversationShare.shared_with == user_id,
            )
        )
        .all()
    )
    conversation_shares_dict = []
    for share_obj, header in conversation_shares:
        share_dict = sql_alchemy_to_dict(share_obj)
        share_dict["conversation_header"] = header
        conversation_shares_dict.append(share_dict)

    return conversation_shares_dict


def get_all_shared_by_user(user_id: str) -> List[ConversationShare]:
    return (
        session.query(ConversationShare)
        .filter(ConversationShare.shared_by == user_id)
        .all()
    )


def create(
    conversation_id: str,
    shared_with: str,
    shared_by: str,
    can_copy: bool = False,
    with_commit: bool = True,
) -> ConversationShare:
    share = ConversationShare(
        conversation_id=conversation_id,
        shared_with=shared_with,
        shared_by=shared_by,
        can_copy=can_copy,
    )
    general.add(share, with_commit)
    return share


def create_many(
    conversation_id: str,
    shared_with_user_ids: List[str],
    shared_by: str,
    can_copy: bool = False,
    with_commit: bool = True,
) -> List[ConversationShare]:

    shares = []

    for user_id in shared_with_user_ids:
        share = ConversationShare(
            conversation_id=conversation_id,
            shared_with=user_id,
            shared_by=shared_by,
            can_copy=can_copy,
        )
        general.add(share, with_commit=False)
        shares.append(share)

    general.flush_or_commit(with_commit)
    return shares


def update(
    share_id: str,
    user_id: str,
    can_copy: Optional[bool] = None,
    with_commit: bool = True,
) -> Optional[ConversationShare]:
    share_entity = get(share_id)
    if share_entity is None:
        return None

    if str(share_entity.shared_by) != user_id:
        raise ValueError("You are not allowed to update this sharing context.")

    if can_copy is not None:
        share_entity.can_copy = can_copy

    general.flush_or_commit(with_commit)
    return share_entity


def delete_shared_with(
    conversation_share_id: str, user_id: str, with_commit: bool = True
) -> None:
    session.query(ConversationShare).filter(
        ConversationShare.id == conversation_share_id
    ).filter(ConversationShare.shared_with == user_id).delete()
    general.flush_or_commit(with_commit)


def delete_shared_by_by_conversation_id(
    conversation_id: str, user_id: str, with_commit: bool = True
) -> None:
    session.query(ConversationShare).filter(
        ConversationShare.conversation_id == conversation_id
    ).filter(ConversationShare.shared_by == user_id).delete()
    general.flush_or_commit(with_commit)
