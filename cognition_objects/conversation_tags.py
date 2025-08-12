from typing import Dict, List, Optional, Any

from ..business_objects import general
from ..session import session
from ..models import CognitionConversationTag, CognitionConversationTagAssociation
from ..util import sql_alchemy_to_dict, prevent_sql_injection
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.types import Boolean
from sqlalchemy import or_


BLACKLIST_CONVERSATION_TAG_ASSOCIATION = {"id", "conversation_id"}


def get(tag_id: str) -> CognitionConversationTag:
    return (
        session.query(CognitionConversationTag)
        .filter(
            CognitionConversationTag.id == tag_id,
        )
        .first()
    )


def get_all_by_user(user_id: str) -> List[CognitionConversationTag]:
    return (
        session.query(CognitionConversationTag)
        .filter(
            CognitionConversationTag.created_by == user_id,
        )
        .all()
    )


def get_all_relevant(user_id: str, project_id: str):
    return (
        session.query(CognitionConversationTag)
        .filter(
            CognitionConversationTag.created_by == user_id,
            or_(
                # global_tag is boolean true
                CognitionConversationTag.config["global_tag"].astext.cast(Boolean),
                # use_for_projects contains  project_id
                CognitionConversationTag.config["use_for_projects"].contains(
                    [project_id]
                ),
            ),
        )
        .all()
    )


def create(
    user_id: str,
    name: str,
    config: Dict[str, Any],
    with_commit: bool = True,
) -> CognitionConversationTag:
    tag: CognitionConversationTag = CognitionConversationTag(
        created_by=user_id,
        name=name,
        config=config,
    )
    general.add(tag, with_commit)
    return tag


def update(
    user_id: str,
    tag_id: str,
    name: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    with_commit: bool = True,
) -> CognitionConversationTag:
    tag_entity = get(tag_id)
    if tag_entity is None:
        return
    if str(tag_entity.created_by) != user_id:
        raise ValueError("You are not allowed to update this tag.")
    if name is not None:
        tag_entity.name = name
    if config is not None and len(config) > 0:
        for key, value in config.items():
            if value is None:
                tag_entity.config.pop(key, None)
            else:
                tag_entity.config[key] = value
        flag_modified(tag_entity, "config")
    general.flush_or_commit(with_commit)
    return tag_entity


def delete(tag_id: str, with_commit: bool = True) -> None:
    session.query(CognitionConversationTag).filter(
        CognitionConversationTag.id == tag_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_many(tag_ids: List[str], with_commit: bool = True) -> None:
    session.query(CognitionConversationTag).filter(
        CognitionConversationTag.id.in_(tag_ids),
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)


def create_association(
    conversation_id: str,
    tag_id: str,
    with_commit: bool = True,
) -> None:
    association = CognitionConversationTagAssociation(
        conversation_id=conversation_id,
        tag_id=tag_id,
    )
    general.add(association, with_commit)


def delete_association(
    conversation_id: str,
    tag_id: str,
    with_commit: bool = True,
) -> None:
    session.query(CognitionConversationTagAssociation).filter(
        CognitionConversationTagAssociation.conversation_id == conversation_id,
        CognitionConversationTagAssociation.tag_id == tag_id,
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)


def get_lookup_by_conversation_ids(
    conversation_ids: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    associations = (
        session.query(CognitionConversationTagAssociation)
        .filter(
            CognitionConversationTagAssociation.conversation_id.in_(conversation_ids)
        )
        .all()
    )
    tag_lookup: Dict[str, List[Dict[str, Any]]] = {}

    for association in associations:
        if str(association.conversation_id) not in tag_lookup:
            tag_lookup[str(association.conversation_id)] = []
        tag_lookup[str(association.conversation_id)].append(
            sql_alchemy_to_dict(
                association, column_blacklist=BLACKLIST_CONVERSATION_TAG_ASSOCIATION
            )
        )
    return tag_lookup


def get_tag_counts(project_id: str, user_id: str) -> Dict[str, int]:

    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    user_id = prevent_sql_injection(user_id, isinstance(user_id, str))

    query = f"""
    SELECT json_object_agg(tid,t_count)
    FROM (
        SELECT COALESCE(cta.tag_id::TEXT,'<untagged>') tid, COUNT(*) t_count
        FROM cognition.conversation C
        LEFT JOIN cognition.conversation_tag_association cta
            ON c.id = cta.conversation_id
        WHERE c.project_id = '{project_id}' AND c.created_by = '{user_id}'
        group BY cta.tag_id 
    ) x """

    value = general.execute_first(query)
    if value and value[0]:
        return value[0]
    return {}
