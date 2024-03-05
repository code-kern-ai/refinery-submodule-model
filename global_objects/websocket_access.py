from typing import Dict
from datetime import datetime, timedelta

from ..business_objects import general
from ..session import session
from ..models import GlobalWebsocketAccess


def get(id: str) -> GlobalWebsocketAccess:
    return (
        session.query(GlobalWebsocketAccess)
        .filter(
            GlobalWebsocketAccess.id == id,
        )
        .first()
    )


def __create(
    config: Dict[str, str],
    user_id: str,
    with_commit: bool = True,
) -> GlobalWebsocketAccess:
    obj = GlobalWebsocketAccess(
        config=config,
        created_by=user_id,
    )
    general.add(obj, with_commit)

    return obj


def create_for_cognition_code_runner(
    project_id: str,
    conversation_id: str,
    step_id_or_type: str,  # e.g. question enrichment if block will get only the type
    user_id: str,
    with_commit: bool = True,
) -> Dict[str, str]:
    config = {
        "project_id": project_id,
        "conversation_id": conversation_id,
        "step_id_or_type": step_id_or_type,
    }
    obj = __create(config, user_id, with_commit)

    config["websocket_auth_key"] = str(obj.id)
    config["user_id"] = user_id

    return config


def delete(id: str, with_commit: bool = True) -> None:
    session.query(GlobalWebsocketAccess).filter(
        GlobalWebsocketAccess.id == id,
    ).delete()
    general.flush_or_commit(with_commit)


def clean_dead_entries(with_commit: bool = True) -> None:
    # | => overloaded operator for or
    # & => overloaded operator for and

    session.query(GlobalWebsocketAccess).filter(
        (
            (GlobalWebsocketAccess.created_at < datetime.now() - timedelta(days=1))
            & (GlobalWebsocketAccess.in_use == False)
        )
        | (GlobalWebsocketAccess.created_at < datetime.now() - timedelta(days=2))
    ).delete()
    general.flush_or_commit(with_commit)
