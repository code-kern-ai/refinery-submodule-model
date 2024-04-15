from typing import Dict, List, Set, Any, Union, Optional

from . import general
from .payload import get_base_query_valid_labels_manual
from .. import models, enums
from ..models import InformationSource, LabelingTask
from ..session import session
from sqlalchemy.sql.expression import cast
import sqlalchemy

from sqlalchemy.engine.row import Row

from ..util import prevent_sql_injection


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


def get_task_and_label_by_ids_and_type(
    project_id: str, task_ids: List[str], task_type: enums.LabelingTaskType
) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
    if len(task_ids) == 0:
        return []
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    id_filter = "'" + "','".join(task_ids) + "'"
    query = f"""
    SELECT array_agg(row_to_json(all_rows))
    FROM(
        SELECT 
            lt.id, 
            lt.name,
            att.name as attribute_name,
            array_agg(
                json_build_object(
                    'id', ltl.id,
                    'name', ltl.name,
                    'color', ltl.color)) labels
        FROM labeling_task lt
        LEFT JOIN attribute att
            ON lt.project_id = att.project_id AND lt.attribute_id = att.id
        LEFT JOIN labeling_task_label ltl
            ON lt.project_id = ltl.project_id AND lt.id = ltl.labeling_task_id
        WHERE lt.project_id = '{project_id}'
            AND lt.id IN ({id_filter})
            AND lt.task_type = '{task_type.value}'
        GROUP BY lt.id, lt.name, att.name
    ) all_rows
    """
    value = general.execute_first(query)
    if value:
        return value[0]
    return []


def get_labeling_tasks_by_project_id_full(project_id: str) -> Row:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    WITH attribute_select AS (	
        SELECT id, jsonb_build_object('id',id,'name', NAME,'relative_position', relative_position, 'data_type', data_Type) a_data
        FROM attribute a
        WHERE project_id = '{project_id}'
    ),
    label_select AS (	
        SELECT labeling_Task_id, jsonb_build_object('edges',array_agg(jsonb_build_object('node',jsonb_build_object('id',id,'name', NAME,'color', color, 'hotkey', hotkey)))) l_data
        FROM labeling_task_label ltl
        WHERE project_id = '{project_id}'
        GROUP BY 1
    ), 
    is_select AS (
        SELECT labeling_task_id, jsonb_build_object('edges',array_agg(jsonb_build_object('node',jsonb_build_object('id',id,'type', type,'return_type', return_type, 'description', description,'name',NAME)))) i_data
        FROM information_source _is
        WHERE project_id = '{project_id}'
        GROUP BY 1
    )

    SELECT 
        '{project_id}' id,
        jsonb_build_object('edges',array_agg(jsonb_build_object('node', lt_data))) labeling_tasks
    FROM (
        SELECT 
            jsonb_build_object(
                'id',lt.id,
                'name', NAME,
                'task_target', task_target, 
                'task_type', task_type, 
                'attribute',a.a_data,
                'labels',COALESCE(l.l_data,jsonb_build_object('edges',ARRAY[]::jsonb[])),
                'information_sources',COALESCE(i.i_data,jsonb_build_object('edges',ARRAY[]::jsonb[]))
            ) lt_data
        FROM labeling_task lt
        LEFT JOIN attribute_select a
            ON lt.attribute_id = a.id
        LEFT JOIN label_select l
            ON l.labeling_Task_id = lt.id
        LEFT JOIN is_select i
            ON i.labeling_task_id = lt.id
        WHERE project_id = '{project_id}'
    ) x """
    return general.execute_first(query)


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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    labeling_task_id = prevent_sql_injection(
        labeling_task_id, isinstance(labeling_task_id, str)
    )
    label_id = prevent_sql_injection(label_id, isinstance(label_id, str))
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))
    record_id = prevent_sql_injection(record_id, isinstance(record_id, str))
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
    record_id = prevent_sql_injection(record_id, isinstance(record_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))
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


def get_labeling_task_name_by_label_id(project_id: str) -> Dict:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    results: List = general.execute_all(
        f"""SELECT ltl.id::TEXT, lt.name
        FROM labeling_task_label ltl 
        INNER JOIN labeling_task lt 
            ON ltl.project_id = lt.project_id AND ltl.labeling_task_id = lt.id
        WHERE lt.project_id = '{project_id}'
        """
    )
    return {result[0]: result[1] for result in results}


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
            task_target=(
                enums.LabelingTaskTarget.ON_WHOLE_RECORD.value
                if not attribute_id
                else enums.LabelingTaskTarget.ON_ATTRIBUTE.value
            ),
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
