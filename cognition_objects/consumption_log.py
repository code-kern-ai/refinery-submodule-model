from typing import Dict, List, Optional, Tuple, Any

from datetime import datetime

from submodules.model import enums

from ..cognition_objects import message
from ..business_objects import general
from ..session import session
from ..models import CognitionConsumptionLog


def get(project_id: str, log_id: str) -> CognitionConsumptionLog:
    return (
        session.query(CognitionConsumptionLog)
        .filter(
            CognitionConsumptionLog.project_id == project_id,
            CognitionConsumptionLog.id == log_id,
        )
        .first()
    )


def get_all_by_project_id(
    project_id: str,
) -> List[CognitionConsumptionLog]:
    return (
        session.query(CognitionConsumptionLog)
        .filter(
            CognitionConsumptionLog.project_id == project_id,
        )
        .order_by(CognitionConsumptionLog.created_at.asc())
        .all()
    )


def get_all_by_project_id_for_year(
    project_id: str,
    year: int,
) -> List[CognitionConsumptionLog]:
    return (
        session.query(CognitionConsumptionLog)
        .filter(
            CognitionConsumptionLog.project_id == project_id,
            CognitionConsumptionLog.created_at >= datetime(year, 1, 1),
            CognitionConsumptionLog.created_at < datetime(year + 1, 1, 1),
        )
        .order_by(CognitionConsumptionLog.created_at.asc())
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    conversation_id: str,
    message_id: str,
    state: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionConsumptionLog:
    conversation: CognitionConsumptionLog = CognitionConsumptionLog(
        project_id=project_id,
        created_by=user_id,
        conversation_id=conversation_id,
        message_id=message_id,
        created_at=created_at,
        state=state,
    )
    general.add(conversation, with_commit)
    return conversation


def update(
    project_id: str,
    log_id: str,
    strategy_id: Optional[str] = None,
    complexity: Optional[str] = None,
    state: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionConsumptionLog:
    log = get(project_id=project_id, log_id=log_id)
    if strategy_id is not None:
        log.strategy_id = strategy_id
    if complexity is not None:
        log.complexity = complexity
    if state is not None:
        log.state = state
    general.flush_or_commit(with_commit)
    return log
