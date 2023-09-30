from typing import List, Optional, Dict, Any
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


def get_all_by_message_id_until_step(
    message_id: str,
    strategy_step_type: str,
    strategy_step_id: str,
) -> List[PipelineLogs]:
    pipeline_logs = (
        session.query(PipelineLogs)
        .filter(PipelineLogs.message_id == message_id)
        .order_by(PipelineLogs.created_at.asc())
        .all()
    )

    pipeline_logs_until_step = []
    for pipeline_log in pipeline_logs:
        pipeline_logs_until_step.append(pipeline_log)
        if strategy_step_id:
            if (
                pipeline_log.strategy_step_type == strategy_step_type
                and pipeline_log.strategy_step_id == strategy_step_id
            ):
                break
        else:
            if pipeline_log.strategy_step_type == strategy_step_type:
                break

    return pipeline_logs_until_step


def create(
    message_id: str,
    project_id: str,
    user_id: str,
    content: str,
    pipeline_step_type: str,
    strategy_step_type: str,
    strategy_step_id: str,
    has_error: bool,
    time_elapsed: float,
    record_dict_diff_previous: Dict[str, Any],
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> PipelineLogs:
    log = PipelineLogs(
        project_id=project_id,
        message_id=message_id,
        created_by=user_id,
        content=content,
        created_at=created_at,
        pipeline_step_type=pipeline_step_type,
        strategy_step_type=strategy_step_type,
        strategy_step_id=strategy_step_id,
        has_error=has_error,
        time_elapsed=time_elapsed,
        record_dict_diff_previous=record_dict_diff_previous,
    )

    general.add(log, with_commit)

    return log
