from operator import or_
from typing import List, Optional
from ..business_objects import general
from ..session import session
from ..models import ConversationGlobalShare
from submodules.model.util import sql_alchemy_to_dict


def get_by_conversation(conversation_id: str) -> List[ConversationGlobalShare]:
    return (
        session.query(ConversationGlobalShare)
        .filter(ConversationGlobalShare.conversation_id == conversation_id)
        .first()
    )


def create(
    conversation_id: str,
    shared_by: str,
    with_commit: bool = True,
) -> ConversationGlobalShare:
    global_share = ConversationGlobalShare(
        conversation_id=conversation_id, shared_by=shared_by
    )
    general.add(global_share, with_commit)
    return global_share


def delete_by_conversation(
    conversation_id: str, user_id: str, with_commit: bool = True
):
    (
        session.query(ConversationGlobalShare)
        .filter(
            ConversationGlobalShare.conversation_id == conversation_id,
            ConversationGlobalShare.shared_by == user_id,
        )
        .delete()
    )
    if with_commit:
        session.commit()
