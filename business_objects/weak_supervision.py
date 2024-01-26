from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..models import (
    LabelingTask,
    LabelingTaskLabel,
    RecordLabelAssociation,
    WeakSupervisionTask,
    RecordLabelAssociationToken,
)

from .. import enums
from ..business_objects import general
from ..session import session
from ..util import prevent_sql_injection


def get_task(project_id: str, ws_task_id: str) -> WeakSupervisionTask:
    return (
        session.query(WeakSupervisionTask)
        .filter(
            WeakSupervisionTask.project_id == project_id,
            WeakSupervisionTask.id == ws_task_id,
        )
        .first()
    )


def get_all(project_id: str) -> List[WeakSupervisionTask]:
    return (
        session.query(WeakSupervisionTask)
        .filter(WeakSupervisionTask.project_id == project_id)
        .all()
    )


def get_current_weak_supervision_run(project_id: str) -> WeakSupervisionTask:
    return (
        session.query(WeakSupervisionTask)
        .filter(
            WeakSupervisionTask.project_id == project_id,
        )
        .order_by(WeakSupervisionTask.created_at.desc())
        .first()
    )


def create_task(
    project_id: str,
    created_by: str,
    selected_information_sources: Optional[str] = None,
    selected_labeling_tasks: Optional[str] = None,
    state: Optional[str] = None,
    created_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    distinct_records: Optional[int] = None,
    result_count: Optional[int] = None,
    with_commit: bool = False,
) -> WeakSupervisionTask:
    task = WeakSupervisionTask(
        project_id=project_id,
        created_by=created_by,
        state=enums.PayloadState.CREATED.value,
    )
    if state:
        task.state = state
    if selected_information_sources:
        task.selected_information_sources = selected_information_sources
    if selected_labeling_tasks:
        task.selected_labeling_tasks = selected_labeling_tasks
    if created_at:
        task.created_at = created_at
    if finished_at:
        task.finished_at = finished_at
    if distinct_records:
        task.distinct_records = distinct_records
    if result_count:
        task.result_count = result_count
    general.add(task, with_commit)
    return task


def update_weak_supervision_task_stats(
    project_id: str, weak_supervision_task_id: str, with_commit: bool = False
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    weak_supervision_task_id = prevent_sql_injection(
        weak_supervision_task_id, isinstance(weak_supervision_task_id, str)
    )
    task: WeakSupervisionTask = (
        session.query(WeakSupervisionTask)
        .filter(
            WeakSupervisionTask.id == weak_supervision_task_id,
            WeakSupervisionTask.project_id == project_id,
        )
        .first()
    )
    if not task:
        raise ValueError(
            f"Weak Supervision task id {weak_supervision_task_id} could not be found"
        )
    count_query = f"""
    SELECT COUNT(*) result_count, COUNT(DISTINCT record_id) distinct_records
    FROM record_label_association rla
    WHERE rla.project_id = '{project_id}' 
    AND rla.weak_supervision_id = '{weak_supervision_task_id}' 
    AND rla.source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}'
    """

    count_results = general.execute_first(count_query)
    task.state = enums.PayloadState.FINISHED.value
    task.finished_at = datetime.now()
    task.distinct_records = count_results.distinct_records
    task.result_count = count_results.result_count
    general.flush_or_commit(with_commit)


def update_state(
    project_id: str,
    weak_supervision_task_id: str,
    state: str,
    with_commit: bool = False,
) -> None:
    task = get_task(project_id, weak_supervision_task_id)
    if task:
        task.state = state
        if state == enums.PayloadState.FINISHED.value:
            task.finished_at = datetime.now()
        general.flush_or_commit(with_commit)
    else:
        raise ValueError(
            f"Weak Supervision task id {weak_supervision_task_id} could not be found"
        )


def store_data(
    project_id: str,
    labeling_task_id: str,
    user_id: str,
    results: Dict[str, Tuple],
    task_type: str,
    weak_supervision_task_id: str,
    with_commit: bool = False,
) -> None:
    session.query(RecordLabelAssociation).filter(
        RecordLabelAssociation.source_type == enums.LabelSource.WEAK_SUPERVISION.value,
        RecordLabelAssociation.labeling_task_label_id == LabelingTaskLabel.id,
        LabelingTaskLabel.labeling_task_id == LabelingTask.id,
        LabelingTask.id == labeling_task_id,
    ).delete(synchronize_session="fetch")
    if task_type == enums.LabelingTaskType.CLASSIFICATION.value:
        for record_id, association_dict_list in results.items():
            record_label_associations = [
                RecordLabelAssociation(
                    record_id=record_id,
                    project_id=project_id,
                    labeling_task_label_id=association_dict["label_id"],
                    confidence=association_dict["confidence"],
                    weak_supervision_id=weak_supervision_task_id,
                    source_type=enums.LabelSource.WEAK_SUPERVISION.value,
                    return_type=enums.InformationSourceReturnType.RETURN.value,
                    created_by=user_id,
                )
                for association_dict in association_dict_list
            ]
            general.add_all(record_label_associations, with_commit=False)
    else:
        for record_id, association_dict_list in results.items():
            record_label_associations = [
                RecordLabelAssociation(
                    record_id=record_id,
                    project_id=project_id,
                    labeling_task_label_id=association_dict["label_id"],
                    confidence=association_dict["confidence"],
                    weak_supervision_id=weak_supervision_task_id,
                    source_type=enums.LabelSource.WEAK_SUPERVISION.value,
                    return_type=enums.InformationSourceReturnType.YIELD.value,
                    tokens=[
                        RecordLabelAssociationToken(
                            project_id=project_id,
                            token_index=index,
                            is_beginning_token=id_ == 0,
                        )
                        for id_, index in enumerate(
                            range(
                                association_dict["token_index_start"],
                                association_dict["token_index_end"] + 1,
                            )
                        )
                    ],
                    created_by=user_id,
                )
                for association_dict in association_dict_list
            ]
            general.add_all(record_label_associations)
    general.flush_or_commit(with_commit)
