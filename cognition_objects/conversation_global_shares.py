from operator import or_
from typing import List, Optional, Dict, Any
from ..business_objects import general
from ..session import session
from ..models import CognitionConversation, ConversationGlobalShare
from submodules.model.util import sql_alchemy_to_dict


def get(conversation_global_share_id: str) -> Optional[ConversationGlobalShare]:
    return (
        session.query(ConversationGlobalShare)
        .filter(ConversationGlobalShare.id == conversation_global_share_id)
        .first()
    )


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
        general.flush_or_commit(with_commit)


def get_by_user(project_id: str, user_id: str) -> List[Dict[str, Any]]:
    conversation_global_shares = (
        session.query(ConversationGlobalShare, CognitionConversation.header)
        .join(
            CognitionConversation,
            ConversationGlobalShare.conversation_id == CognitionConversation.id,
        )
        .filter(ConversationGlobalShare.shared_by == user_id)
        .filter(CognitionConversation.project_id == project_id)
        .all()
    )
    conversation_global_shares_dict = []
    for share_obj, header in conversation_global_shares:
        share_dict = sql_alchemy_to_dict(share_obj)
        share_dict["conversation_header"] = header
        conversation_global_shares_dict.append(share_dict)
    return conversation_global_shares_dict
