from typing import List, Optional, Dict, Any, Tuple
from ..business_objects import general
from ..session import session
from ..models import CognitionPipelineLogs, CognitionMessage
from datetime import datetime
from .. import enums
from ..util import prevent_sql_injection


def get_all_by_message_id(
    project_id: str, message_id: str, user_id: Optional[str] = None
) -> List[CognitionPipelineLogs]:

    query = session.query(CognitionPipelineLogs).filter(
        CognitionPipelineLogs.project_id == project_id,
        CognitionPipelineLogs.message_id == message_id,
    )
    if user_id:
        query = query.filter(CognitionPipelineLogs.created_by == user_id)
    return query.order_by(CognitionPipelineLogs.created_at.asc()).all()


def get_all_by_conversation_id(
    project_id: str, conversation_id: str, user_id: Optional[str] = None
) -> List[CognitionPipelineLogs]:
    query = (
        session.query(CognitionPipelineLogs)
        .join(
            CognitionMessage,
            (CognitionMessage.project_id == CognitionPipelineLogs.project_id)
            & (CognitionMessage.id == CognitionPipelineLogs.message_id),
        )
        .filter(
            CognitionPipelineLogs.project_id == project_id,
            CognitionMessage.conversation_id == conversation_id,
        )
    )
    if user_id:
        query = query.filter(CognitionPipelineLogs.created_by == user_id)
    return query.order_by(CognitionPipelineLogs.created_at.asc()).all()


def get_all_by_message_id_until_step(
    project_id: str,
    message_id: str,
    pipeline_step_type: str,
    strategy_step_type: str,
    strategy_step_id: Optional[str] = None,
    iteration_number: Optional[int] = None,
) -> List[CognitionPipelineLogs]:

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
        if strategy_step_id and pipeline_log.iteration_number == iteration_number:
            if (
                pipeline_log.strategy_step_type == strategy_step_type
                and pipeline_log.strategy_step_id == strategy_step_id
            ):
                break
        else:
            if (
                pipeline_log.pipeline_step_type == pipeline_step_type
                and pipeline_log.iteration_number == iteration_number
            ):
                break

    return pipeline_logs_until_step


def get_all_by_message_id_and_pipeline_step_type(
    project_id: str,
    message_id: str,
    pipeline_step_type: str,
) -> List[CognitionPipelineLogs]:
    return (
        session.query(CognitionPipelineLogs)
        .filter(
            CognitionPipelineLogs.project_id == project_id,
            CognitionPipelineLogs.message_id == message_id,
            CognitionPipelineLogs.pipeline_step_type == pipeline_step_type,
        )
        .order_by(CognitionPipelineLogs.created_at.asc())
        .all()
    )


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
    record_dict_diff_previous_new: Dict[str, Any],
    scope_dict_diff_previous_new: Dict[str, Any],
    skipped_step: Optional[bool] = None,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    count_pipeline_step_type: Optional[bool] = False,
    count_strategy_step_id: Optional[bool] = False,
) -> CognitionPipelineLogs:

    if count_pipeline_step_type or count_strategy_step_id:
        number_logs = len(
            get_all_by_message_id_and_pipeline_step_type(
                project_id, message_id, enums.PipelineStep.ROUTE_STRATEGY.value
            )
        )
        if count_pipeline_step_type:
            iteration_number = number_logs
        if count_strategy_step_id:
            iteration_number = number_logs - 1
    else:
        iteration_number = None

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
        record_dict_diff_new=record_dict_diff_previous_new,
        scope_dict_diff_new=scope_dict_diff_previous_new,
        skipped_step=skipped_step,
        iteration_number=iteration_number,
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


def get_all_by_messages_ids(project_id: str, message_ids: List[str]):
    return (
        session.query(CognitionPipelineLogs)
        .filter(
            CognitionPipelineLogs.project_id == project_id,
            CognitionPipelineLogs.message_id.in_(message_ids),
        )
        .order_by(CognitionPipelineLogs.created_at.asc())
        .all()
    )


# migration method to be removed in release after next
def get_logs_to_be_migrated_to_new_structure() -> List[Tuple[str, str, str, str, str]]:
    query = """
    SELECT x.id::TEXT conversation_id,m.id::TEXT message_id, pl.id::TEXT log_id, pl.record_dict_diff_previous_message, pl.scope_dict_diff_previous_message
    FROM (
        SELECT DISTINCT c.id, c.project_id
        FROM cognition.conversation c
        INNER JOIN cognition.message m
            ON c.id = m.conversation_id AND c.project_id = m.project_id
        INNER JOIN cognition.pipeline_logs pl
            ON m.project_id = pl.project_id AND m.id = pl.message_id
        WHERE pl.scope_dict_diff_previous_message::TEXT != '"null"' OR pl.record_dict_diff_previous_message::TEXT != '"null"'
        LIMIT 50 -- max conversations per chunk
    )x
    INNER JOIN cognition.message m
        ON m.conversation_id = x.id AND m.project_id = x.project_id
    INNER JOIN cognition.pipeline_logs pl
        ON m.project_id = pl.project_id AND m.id = pl.message_id
    ORDER BY pl.created_at ASC
    """

    values = general.execute_all(query)
    if values:
        return [(value[0], value[1], value[2], value[3], value[4]) for value in values]
    return []


def update_to_new_diff_structure(
    log_id: str,
    new_record_dict_diff: List[Dict[str, Any]],
    new_scope_dict_diff: List[Dict[str, Any]],
    with_commit: bool = False,
):
    session.query(CognitionPipelineLogs).filter(
        CognitionPipelineLogs.id == log_id
    ).update(
        {
            CognitionPipelineLogs.record_dict_diff_previous_message: "null",
            CognitionPipelineLogs.scope_dict_diff_previous_message: "null",
            CognitionPipelineLogs.record_dict_diff_new: new_record_dict_diff,
            CognitionPipelineLogs.scope_dict_diff_new: new_scope_dict_diff,
        },
        synchronize_session=False,
    )

    if with_commit:
        general.commit()


def get_error_and_time_elapsed_by_conversation_ids(
    project_id: str,
    conversation_ids: List[str],
) -> Dict[str, CognitionPipelineLogs]:
    if not conversation_ids:
        return {}
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    conversation_ids = [
        prevent_sql_injection(conversation_id, isinstance(conversation_id, str))
        for conversation_id in conversation_ids
    ]
    conversation_where = (
        " AND m.conversation_id IN ('" + "','".join(conversation_ids) + "')"
    )
    query = f"""
    SELECT jsonb_object_agg(id, jsonb_build_object('logs_have_error', has_error, 'time_logs_elapsed', time_elapsed))
    FROM (
        SELECT m.id, SUM(pl.has_error::INT) > 0 has_error,SUM(pl.time_elapsed) time_elapsed
        FROM cognition.message m
        INNER JOIN cognition.pipeline_logs pl
            ON m.project_id = pl.project_id AND m.id = pl.message_id
        WHERE m.project_Id = '{project_id}'{conversation_where}
        GROUP BY m.id
    )x"""
    conversation_info = general.execute_first(query)
    if conversation_info and conversation_info[0]:
        return conversation_info[0]
    return {}
