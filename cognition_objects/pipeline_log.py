from typing import List, Optional, Dict, Any
from ..business_objects import general
from ..session import session
from ..models import CognitionPipelineLogs
from datetime import datetime


def get_all_by_message_id(
    project_id: str, message_id: str
) -> List[CognitionPipelineLogs]:
    return (
        session.query(CognitionPipelineLogs)
        .filter(
            CognitionPipelineLogs.project_id == project_id,
            CognitionPipelineLogs.message_id == message_id,
        )
        .order_by(CognitionPipelineLogs.created_at.asc())
        .all()
    )


def get_all_by_message_id_until_step(
    project_id: str,
    message_id: str,
    pipeline_step_type: str,
    strategy_step_type: str,
    strategy_step_id: Optional[str] = None,
) -> List[CognitionPipelineLogs]:
    
    # TODO: this likely contains the error why the record_dict is not updated with further increments of steps

    pipeline_logs: List[CognitionPipelineLogs] = (
        session.query(CognitionPipelineLogs)
        .filter(
            CognitionPipelineLogs.project_id == project_id,
            CognitionPipelineLogs.message_id == message_id,
        )
        .order_by(CognitionPipelineLogs.created_at.asc())
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
            # I think the error is here, because there can be multiple instances of this pipeline_step_type
            if pipeline_log.pipeline_step_type == pipeline_step_type:
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
    scope_dict_diff_previous: Dict[str, Any],
    skipped_step: Optional[bool] = None,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionPipelineLogs:
    log = CognitionPipelineLogs(
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
        record_dict_diff_previous_message=record_dict_diff_previous,
        scope_dict_diff_previous_message=scope_dict_diff_previous,
        skipped_step=skipped_step,
    )

    general.add(log, with_commit)

    return log


def delete_all_by_message_id(
    project_id: str,
    message_id: str,
    with_commit: bool = True,
) -> None:
    session.query(CognitionPipelineLogs).filter(
        CognitionPipelineLogs.project_id == project_id,
        CognitionPipelineLogs.message_id == message_id,
    ).delete()

    general.flush_or_commit(with_commit)
