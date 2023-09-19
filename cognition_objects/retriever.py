from datetime import datetime
from typing import Dict, List, Optional, Tuple

from . import message
from ..business_objects import general
from ..session import session
from ..models import Retriever
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(retriever_id: str) -> Retriever:
    return session.query(Retriever).filter(Retriever.id == retriever_id).first()


def get_all_by_project_id_and_strategy_step_id(
    project_id: str, strategy_step_id: str
) -> List[Retriever]:
    return (
        session.query(Retriever)
        .filter(Retriever.project_id == project_id)
        .filter(Retriever.strategy_step_id == strategy_step_id)
        .order_by(Retriever.created_at.asc())
        .all()
    )


def create(
    project_id: str,
    strategy_step_id: str,
    user_id: str,
    name: str,
    description: str,
    source_code: str,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> Retriever:
    retriever: Retriever = Retriever(
        project_id=project_id,
        strategy_step_id=strategy_step_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        source_code=source_code,
        enabled=False,
    )
    general.add(retriever, with_commit)

    return retriever


def update(
    retriever_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    source_code: Optional[str] = None,
    with_commit: bool = True,
) -> Retriever:
    retriever: Retriever = get(retriever_id)
    if name:
        retriever.name = name
    if description:
        retriever.description = description
    if source_code:
        retriever.source_code = source_code
    general.flush_or_commit(with_commit)
    return retriever


def delete(retriever_id: str, with_commit: bool = True) -> None:
    session.query(Retriever).filter(
        Retriever.id == retriever_id,
    ).delete()
    general.flush_or_commit(with_commit)
