from typing import List, Optional
from ..business_objects import general
from ..session import session
from ..models import PipelineLogs


def get_all_by_message_id(message_id: str) -> List[PipelineLogs]:
    return (
        session.query(PipelineLogs)
        .filter(PipelineLogs.message_id == message_id)
        .order_by(PipelineLogs.created_at.asc())
        .all()
    )


def create(
    message_id: str,
    project_id: str,
    user_id: str,
    content: str,
    step_type: str,
    step_id: str,
    has_error: bool,
    time_elapsed: float,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> PipelineLogs:
    log = PipelineLogs(
        project_id=project_id,
        message_id=message_id,
        created_by=user_id,
        content=content,
        created_at=created_at,
        step_type=step_type,
        step_id=step_id,
        has_error=has_error,
        time_elapsed=time_elapsed,
    )

    general.add(log, with_commit)

    return log
