from typing import Dict, List, Set, Any, Optional

from . import general
from .payload import get_base_query_valid_labels_manual
from .. import models, enums
from ..models import InformationSource, LabelingTask
from ..session import session
from sqlalchemy.sql.expression import cast
import sqlalchemy


def get(project_id: str, task_id: str) -> LabelingTask:
    return (
        session.query(LabelingTask)
        .filter(LabelingTask.project_id == project_id, LabelingTask.id == task_id)
        .first()
    )


def get_all(project_id: str) -> List[LabelingTask]:
    return (
        session.query(models.LabelingTask)
        .filter(
            models.LabelingTask.project_id == project_id,
        )
        .all()
    )


def get_task_name_id_dict(project_id: str) -> Dict[str, str]:
    labeling_tasks = get_all(project_id)
    return {labeling_task.name: labeling_task.id for labeling_task in labeling_tasks}


def get_labeling_task_by_name(project_id: str, task_name: str) -> LabelingTask:
    return (
        session.query(LabelingTask)
        .filter(LabelingTask.project_id == project_id, LabelingTask.name == task_name)
        .first()
    )


def get_labeling_tasks_by_selected_sources(project_id: str) -> List[LabelingTask]:
    return (
        session.query(LabelingTask)
        .join(
            InformationSource,
            (LabelingTask.id == InformationSource.labeling_task_id)
            & (LabelingTask.project_id == InformationSource.project_id),
        )
        .filter(
            InformationSource.project_id == project_id,
            InformationSource.is_selected == True,
        )
        .group_by(LabelingTask.id)
        .all()
    )


def get_selected_labeling_task_names(project_id: str) -> str:
    labeling_tasks = get_labeling_tasks_by_selected_sources(project_id)
    if not labeling_tasks:
        return ""
    return ", ".join([str(x.name) for x in labeling_tasks])


def get_record_classifications_manual(
    project_id: str, labeling_task_id: str, label_id: str
) -> Set[str]:
    base_query: str = get_base_query_valid_labels_manual(project_id, labeling_task_id)

    query: str = (
        base_query
        + f"""
    SELECT rla.record_id::TEXT
    FROM valid_rla_ids vri
    INNER JOIN record_label_association rla
        ON vri.rla_id = rla.id
    WHERE rla.project_id = '{project_id}'
        AND rla.labeling_task_label_id = '{label_id}'
    GROUP BY rla.record_id
    """
    )
    return set([r.record_id for r in general.execute_all(query)])


def get_record_classifications(
    project_id: str, label_id: str, label_source: str
) -> Set[str]:
    record_hits: List = (
        session.query(cast(models.RecordLabelAssociation.record_id, sqlalchemy.String))
        .filter(
            models.LabelingTask.project_id == project_id,
            models.LabelingTask.id == models.LabelingTaskLabel.labeling_task_id,
            models.LabelingTaskLabel.id
            == models.RecordLabelAssociation.labeling_task_label_id,
            models.LabelingTaskLabel.id == label_id,
            models.RecordLabelAssociation.source_type == label_source,
        )
        .all()
    )
    return set([record for record, in record_hits])


def get_relevant_extraction_records(project_id: str, task_id: str) -> List[str]:
    base_query: str = get_base_query_valid_labels_manual(project_id, str(task_id))
    query: str = (
        base_query
        + f"""
            SELECT rla.record_id::TEXT 
            FROM valid_rla_ids vri
            INNER JOIN record_label_association rla
                ON vri.rla_id = rla.id
            WHERE rla.project_id = '{project_id}'
            GROUP BY rla.record_id
            """
    )
    return general.execute_all(query)


def get_record_extraction_vector_triplets_manual(
    project_id: str, task_id: str, record_id: str
) -> List[Any]:
    base_query: str = get_base_query_valid_labels_manual(
        project_id, labeling_task_id=task_id, record_id=record_id
    )
    query: str = (
        base_query
        + f"""
        SELECT ltl.name,rlat.token_index,rats.num_token
        FROM valid_rla_ids vri
        INNER JOIN record_label_association rla
            ON vri.rla_id = rla.id
        INNER JOIN labeling_task_label ltl
            ON rla.labeling_task_label_id = ltl.id AND rla.project_id = ltl.project_id
        INNER JOIN labeling_task lt
            ON ltl.labeling_task_id = lt.id AND ltl.project_id = lt.project_id         
        INNER JOIN attribute a
            ON lt.attribute_id = a.id AND lt.project_id = a.project_id
        INNER JOIN record_label_association_token rlat
            ON rla.id = rlat.record_label_association_id
        INNER JOIN record_attribute_token_statistics rats
            ON rla.record_id = rats.record_id AND rats.attribute_id = a.id

        WHERE rla.project_id = '{project_id}'
           AND rla.record_id = '{record_id}'
           AND rats.record_id = '{record_id}'
        """
    )
    return general.execute_all(query)


def get_record_extraction_vector_triplets_weak_supervision(
    record_id: str, task_id: str
) -> List[Any]:
    query: str = f"""
    SELECT ltl.name,rlat.token_index,rats.num_token
    FROM record_label_association rla
    INNER JOIN record_label_association_token rlat
        ON rlat.record_label_association_id = rla.id
    INNER JOIN labeling_task_label ltl
        ON rla.project_id = ltl.project_id AND rla.labeling_task_label_id = ltl.id
    INNER JOIN labeling_task lt
        ON ltl.project_id = lt.project_id AND lt.id = ltl.labeling_task_id
    INNER JOIN record_attribute_token_statistics rats
        ON rla.record_id = rats.record_id AND lt.attribute_id = rats.attribute_id
    WHERE lt.id = '{task_id}' 
        AND rla.source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}' 
        AND rla.record_id = '{record_id}'
    """
    return general.execute_all(query)


def get_labeling_task_by_id_only(task_id: str) -> LabelingTask:
    return session.query(LabelingTask).filter(LabelingTask.id == task_id).first()


def get_labeling_task_by_source_id(source_id: str) -> LabelingTask:
    return (
        session.query(LabelingTask)
        .filter(
            LabelingTask.id == InformationSource.labeling_task_id,
            InformationSource.id == source_id,
        )
        .first()
    )


def create(
    project_id: str,
    attribute_id: str,
    name: str,
    task_target: str,
    task_type: str,
    with_commit: bool = False,
) -> LabelingTask:
    labeling_task: LabelingTask = LabelingTask(
        name=name,
        project_id=project_id,
        attribute_id=attribute_id,
        task_target=task_target,
        task_type=task_type,
    )
    general.add(labeling_task, with_commit)
    return labeling_task


def create_multiple(
    project_id: str,
    attribute_ids: Dict,
    tasks_data: Dict[str, Any],
    with_commit: bool = False,
) -> None:
    tasks: List = []
    for task_name in tasks_data:
        attribute_id: str = attribute_ids.get(
            tasks_data.get(task_name).get("attribute")
        )
        labeling_task: models.LabelingTask = models.LabelingTask(
            name=task_name,
            project_id=project_id,
            attribute_id=attribute_id or None,
            task_target=enums.LabelingTaskTarget.ON_WHOLE_RECORD.value
            if not attribute_id
            else enums.LabelingTaskTarget.ON_ATTRIBUTE.value,
            task_type=enums.LabelingTaskType.CLASSIFICATION.value,
        )
        tasks.append(labeling_task)
    general.add_all(tasks, with_commit)


def update(
    project_id: str,
    task_id: str,
    task_target: str,
    attribute_id: str,
    labeling_task_name: Optional[str] = None,
    labeling_task_type: Optional[str] = None,
    with_commit: bool = False,
) -> None:
    task: LabelingTask = get(project_id, task_id)
    if labeling_task_name is not None:
        task.name = labeling_task_name
    if labeling_task_type is not None:
        task.task_type = labeling_task_type
    task.task_target = task_target
    task.attribute_id = attribute_id
    general.flush_or_commit(with_commit)


def delete(project_id: str, task_id: str, with_commit: bool = False) -> None:
    session.query(LabelingTask).filter(
        LabelingTask.project_id == project_id,
        LabelingTask.id == task_id,
    ).delete()
    general.flush_or_commit(with_commit)
