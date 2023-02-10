from typing import List, Dict, Optional, Tuple, List, Any

from . import general
from ..business_objects import payload
from .. import models, enums
from ..models import LabelingTaskLabel, LabelingTask
from ..session import session


def get_all_ids(project_id: str, labeling_task_id: str) -> List[Any]:
    return (
        session.query(LabelingTaskLabel.id)
        .filter(
            LabelingTaskLabel.project_id == project_id,
            LabelingTaskLabel.labeling_task_id == labeling_task_id,
        )
        .all()
    )


def get_all(project_id: str) -> List[LabelingTaskLabel]:
    return (
        session.query(LabelingTaskLabel)
        .filter(LabelingTaskLabel.project_id == project_id)
        .all()
    )


def get_all_by_task_id(
    project_id: str, labeling_task_id: str
) -> List[LabelingTaskLabel]:
    return (
        session.query(LabelingTaskLabel)
        .filter(
            LabelingTaskLabel.project_id == project_id,
            LabelingTaskLabel.labeling_task_id == labeling_task_id,
        )
        .all()
    )


def get_all_ids_query(project_id: str, labeling_task_id: str) -> Any:
    return session.query(LabelingTaskLabel.id).filter(
        LabelingTaskLabel.project_id == project_id,
        LabelingTaskLabel.labeling_task_id == labeling_task_id,
    )


def get(project_id: str, label_id: str) -> LabelingTaskLabel:
    return (
        session.query(LabelingTaskLabel)
        .filter(
            LabelingTaskLabel.project_id == project_id,
            LabelingTaskLabel.id == label_id,
        )
        .first()
    )


def get_by_name(
    project_id: str, labeling_task_id: str, label_name: str
) -> LabelingTaskLabel:
    return (
        session.query(LabelingTaskLabel)
        .filter(
            LabelingTask.id == LabelingTaskLabel.labeling_task_id,
            LabelingTaskLabel.project_id == project_id,
            LabelingTask.id == labeling_task_id,
            LabelingTaskLabel.name == label_name,
        )
        .first()
    )


def get_label_ids_by_task_and_label_name(project_id: str) -> Dict[str, Dict[str, str]]:
    """Returns dict with task and label information like:
    {
        label_task_name: {
            label_task_label_name: label_task_label_id
        }
        ...
    }
    """
    sql: str = f"SELECT lt.name, ltl.name, ltl.id FROM labeling_task lt JOIN labeling_task_label ltl ON lt.id = ltl.labeling_task_id WHERE lt.project_id = '{project_id}'"
    results: List = general.execute_all(sql)
    return_dict: Dict = {}
    for result in results:
        if result[0] not in return_dict:
            return_dict[result[0]] = {result[1]: result[2]}
        else:
            return_dict[result[0]][result[1]] = result[2]
    return return_dict


def get_labels_by_tasks(project_id: str) -> Dict[str, List[str]]:
    results: List = general.execute_all(
        f"""
        SELECT ltl.name, lt.name
        FROM labeling_task_label ltl 
        INNER JOIN labeling_task lt 
            ON lt.project_id = ltl.project_id AND ltl.labeling_task_id = lt.id 
        WHERE lt.project_id = '{project_id}'
        """
    )

    labels_by_tasks: Dict = {}
    for result in results:
        if result[1] not in labels_by_tasks:
            labels_by_tasks[result[1]] = [result[0]]
        else:
            labels_by_tasks[result[1]].append(result[0])

    return labels_by_tasks


def get_classification_labels_manual(
    project_id: str, labeling_task_id: str
) -> List[str]:
    query: str = payload.get_query_labels_classification_manual(
        project_id, labeling_task_id
    )
    return [label_name for _, label_name in general.execute_all(query)]


def get_extraction_labels(
    project_id: str, labeling_task_id: str, source_type: str
) -> List[Tuple[str, str, List[Any]]]:
    if source_type == enums.LabelSource.MANUAL.value:
        raise ValueError(
            "Source type manual now has it's own function - use get_extraction_labels_manual - use that"
        )
    query: str = payload.get_query_labels_extraction(
        project_id, labeling_task_id, source_type
    )
    return [
        (record_id, label_name, token_list)
        for record_id, label_name, token_list in general.execute_all(query)
    ]


def get_extraction_labels_manual(
    project_id: str, labeling_task_id: str
) -> List[Tuple[str, str, List[Any]]]:
    query: str = payload.get_query_labels_extraction_manual(
        project_id, labeling_task_id
    )
    return [
        (record_id, label_name, token_list)
        for record_id, label_name, token_list in general.execute_all(query)
    ]


def get_label_ids_by_names(labeling_task_id: str, project_id: str) -> Dict[str, str]:
    query = session.query(LabelingTaskLabel.id, LabelingTaskLabel.name).filter(
        LabelingTaskLabel.project_id == project_id,
        LabelingTaskLabel.labeling_task_id == labeling_task_id,
    )

    return {x.name: x.id for x in query.all()}


def create(
    project_id: str,
    name: str,
    labeling_task_id: str,
    label_color: Optional[str] = None,
    label_hotkey: Optional[str] = None,
    with_commit: bool = False,
) -> LabelingTaskLabel:
    if not label_color:
        label_color = "yellow"
    label: LabelingTaskLabel = LabelingTaskLabel(
        name=name,
        project_id=project_id,
        labeling_task_id=labeling_task_id,
        color=label_color,
        hotkey=label_hotkey,
    )
    general.add(label, with_commit)
    return label


def delete(project_id: str, label_id: str, with_commit: bool = False) -> None:
    (
        session.query(LabelingTaskLabel)
        .filter(
            LabelingTaskLabel.project_id == project_id,
            LabelingTaskLabel.id == label_id,
        )
        .delete()
    )
    general.flush_or_commit(with_commit)


def create_labels(
    project_id: str,
    task_ids: Dict[str, str],
    labels_data: Dict[str, List[str]],
    with_commit: bool = False,
) -> None:

    colorOptions = [
        "red",
        "orange",
        "amber",
        "yellow",
        "lime",
        "green",
        "emerald",
        "teal",
        "cyan",
        "sky",
        "blue",
        "indigo",
        "violet",
        "purple",
        "fuchsia",
        "pink",
        "rose",
    ]

    for task_name in labels_data:
        if not labels_data.get(task_name):
            continue

        for idx, labeling_task_label_name in enumerate(labels_data.get(task_name)):
            if labeling_task_label_name is None:
                continue
            colorIdx = idx % len(colorOptions)
            general.add(
                models.LabelingTaskLabel(
                    project_id=project_id,
                    name=labeling_task_label_name,
                    labeling_task_id=task_ids.get(task_name),
                    color=colorOptions[colorIdx],
                ),
                with_commit,
            )
