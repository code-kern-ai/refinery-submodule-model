import time
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple

from sqlalchemy import or_
from sqlalchemy.orm.session import make_transient

from .. import enums
from ..models import (
    RecordLabelAssociation,
    RecordLabelAssociationToken,
    Record,
    LabelingTaskLabel,
    LabelingTask,
)
from ..session import session
from ..business_objects import general, labeling_task_label, labeling_task, payload


def get_all_with_filter(
    project_id: str,
    record_id: str,
    labeling_task_id: str,
    source_type: str,
    created_by: str,
) -> List[RecordLabelAssociation]:
    label_ids_query = labeling_task_label.get_all_ids_query(
        project_id, labeling_task_id
    )

    return (
        session.query(RecordLabelAssociation)
        .filter(
            RecordLabelAssociation.project_id == project_id,
            RecordLabelAssociation.record_id == record_id,
            RecordLabelAssociation.labeling_task_label_id.in_(label_ids_query),
            RecordLabelAssociation.source_type == source_type,
            RecordLabelAssociation.created_by == created_by,
        )
        .all()
    )


def check_any_id_is_source_related(
    project_id: str, record_id: str, association_ids: List[str]
) -> List[str]:
    query = f"""
    SELECT array_agg(source_id::TEXT)
    FROM record_label_association
    WHERE project_id = '{project_id}'
    AND record_id = '{record_id}'
    AND id IN ('{"', '".join(association_ids)}')
    AND source_type = '{enums.LabelSource.INFORMATION_SOURCE.value}'
    """
    values = general.execute_first(query)
    if values:
        return values[0]
    return []


def get_project_ids_with_rlas() -> List[Any]:
    query = f"""
    SELECT project_id::TEXT
    FROM record_label_association
    GROUP BY project_id
    """
    values = general.execute_all(query)
    if not values:
        return []
    return [row[0] for row in values]


def get_labeling_tasks_from_ids(project_id: str, rla_ids: List[str]) -> List[Any]:
    if not rla_ids:
        return []

    rla_id_filter = "'" + "', '".join(rla_ids) + "'"
    query = f"""
    SELECT ltl.labeling_task_id::TEXT
    FROM record_label_association rla
    INNER JOIN labeling_task_label ltl
    ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id
    WHERE rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}'
    AND rla.id IN ({rla_id_filter})
    GROUP BY ltl.labeling_task_id
    """
    values = general.execute_all(query)
    if values:
        return [row[0] for row in values]
    return []


def get_tokens(project_id: str) -> List[Any]:
    query = f"""
        SELECT
            record_label_association.id,
            record_label_association_token.token_index,
            record_label_association_token.is_beginning_token
        FROM
            record_label_association
        INNER JOIN
            record_label_association_token
        ON
            record_label_association.id=record_label_association_token.record_label_association_id
        WHERE
            record_label_association.project_id='{project_id}'
        ;
        """
    return general.execute_all(query)


def get_manual_tokens_by_record_id(
    project_id: str,
    record_id: str,
) -> List[RecordLabelAssociationToken]:
    return (
        session.query(RecordLabelAssociationToken)
        .join(
            RecordLabelAssociation,
            (
                RecordLabelAssociation.id
                == RecordLabelAssociationToken.record_label_association_id
            )
            & (
                RecordLabelAssociationToken.project_id
                == RecordLabelAssociation.project_id
            ),
        )
        .filter(
            RecordLabelAssociation.record_id == record_id,
            RecordLabelAssociation.source_type == enums.LabelSource.MANUAL.value,
            RecordLabelAssociation.project_id == project_id,
        )
        .all()
    )


def get_all(project_id: str) -> List[RecordLabelAssociation]:
    return (
        session.query(RecordLabelAssociation)
        .filter(RecordLabelAssociation.project_id == project_id)
        .all()
    )


def get_latest(project_id: str, top_n: int) -> List[RecordLabelAssociation]:
    return (
        session.query(RecordLabelAssociation)
        .filter(
            RecordLabelAssociation.source_type == enums.LabelSource.MANUAL.value,
            RecordLabelAssociation.project_id == project_id,
        )
        .order_by(RecordLabelAssociation.created_at.desc())
        .limit(top_n)
        .all()
    )


def get_manual_records(project_id: str, labeling_task_id: str) -> List[str]:
    query = f"""
        SELECT record_id::TEXT
        FROM record_label_association rla
        INNER JOIN labeling_task_label ltl
        ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id
        WHERE rla.is_valid_manual_label = TRUE
        AND rla.source_type = '{enums.LabelSource.MANUAL.value}'
        AND rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}'
        AND ltl.labeling_task_id = '{labeling_task_id}'
        GROUP BY record_id
    """
    return general.execute_all(query)


def get_all_classifications_for_information_source(
    project_id: str,
    source_id: str,
) -> List[Tuple[RecordLabelAssociation, LabelingTask, LabelingTaskLabel]]:
    labeling_task_item = labeling_task.get_labeling_task_by_source_id(source_id)
    return (
        session.query(
            RecordLabelAssociation,
            LabelingTask,
            LabelingTaskLabel,
        )
        .filter(
            RecordLabelAssociation.project_id == project_id,
            RecordLabelAssociation.labeling_task_label_id == LabelingTaskLabel.id,
            LabelingTaskLabel.labeling_task_id == LabelingTask.id,
        )
        .filter(
            LabelingTask.id == labeling_task_item.id,
            RecordLabelAssociation.source_type
            != enums.LabelSource.WEAK_SUPERVISION.value,
        )
        .filter(
            RecordLabelAssociation.source_id == source_id,
        )
        .all()
    )


def get_all_extraction_tokens_for_information_source(
    project_id: str,
    source_id: str,
) -> List[
    Tuple[
        RecordLabelAssociation,
        RecordLabelAssociationToken,
        LabelingTask,
        LabelingTaskLabel,
    ]
]:
    labeling_task_item = labeling_task.get_labeling_task_by_source_id(source_id)
    return (
        session.query(
            RecordLabelAssociation,
            RecordLabelAssociationToken,
            LabelingTask,
            LabelingTaskLabel,
        )
        .filter(
            RecordLabelAssociation.id
            == RecordLabelAssociationToken.record_label_association_id,
            RecordLabelAssociation.labeling_task_label_id == LabelingTaskLabel.id,
            LabelingTaskLabel.labeling_task_id == LabelingTask.id,
        )
        .filter(
            LabelingTask.id == labeling_task_item.id,
            RecordLabelAssociation.source_type
            != enums.LabelSource.WEAK_SUPERVISION.value,
        )
        .filter(
            RecordLabelAssociation.source_id == source_id,
        )
        .all()
    )


def count_absolute(task_id: str, record_category: str, label_source: str) -> int:
    return (
        session.query(RecordLabelAssociation)
        .filter(
            RecordLabelAssociation.labeling_task_label_id == task_id,
            RecordLabelAssociation.record_id == Record.id,
            Record.category == record_category,
            RecordLabelAssociation.source_type == label_source,
        )
        .count()
    )


def count_relative(task_id: str, record_category: str, label_source: str) -> int:
    return (
        session.query(RecordLabelAssociation)
        .join(
            Record,
            (RecordLabelAssociation.record_id == Record.id)
            & (RecordLabelAssociation.project_id == Record.project_id),
        )
        .join(
            LabelingTaskLabel,
            (LabelingTaskLabel.id == RecordLabelAssociation.labeling_task_label_id)
            & (RecordLabelAssociation.project_id == LabelingTaskLabel.project_id),
        )
        .filter(
            Record.category == record_category,
            RecordLabelAssociation.source_type == label_source,
            LabelingTaskLabel.labeling_task_id == task_id,
        )
        .count()
    )


def create(
    project_id: str,
    record_id: str,
    labeling_task_label_id: str,
    created_by: str,
    source_type: str,
    return_type: str,
    is_gold_star: bool,
    tokens: Optional[List] = None,
    source_id: Optional[str] = None,
    confidence: Optional[float] = None,
    created_at: Optional[datetime] = None,
    weak_supervision_id: Optional[str] = None,
    is_valid_manual_label: Optional[bool] = None,
    with_commit: bool = False,
) -> RecordLabelAssociation:
    association: RecordLabelAssociation = RecordLabelAssociation(
        record_id=record_id,
        project_id=project_id,
        labeling_task_label_id=labeling_task_label_id,
        source_type=source_type,
        return_type=return_type,
        created_by=created_by,
        is_gold_star=is_gold_star,
    )
    if tokens:
        association.tokens = tokens
    if source_id:
        association.source_id = source_id
    if confidence:
        association.confidence = confidence
    if created_at:
        association.created_at = created_at
    if weak_supervision_id:
        association.weak_supervision_id = weak_supervision_id
    if is_valid_manual_label:
        association.is_valid_manual_label = is_valid_manual_label
    general.add(association, with_commit)
    return association


def import_token_object(
    project_id: str,
    record_label_association_id: str,
    token_index: int,
    is_beginning_token: bool,
    with_commit: bool = False,
) -> RecordLabelAssociationToken:
    token: RecordLabelAssociationToken = RecordLabelAssociationToken(
        project_id=project_id,
        record_label_association_id=record_label_association_id,
        token_index=token_index,
        is_beginning_token=is_beginning_token,
    )
    general.add(token, with_commit)
    return token


def create_token_objects(
    project_id: str, token_start_index: int, token_end_index: int
) -> List[RecordLabelAssociationToken]:
    return [
        RecordLabelAssociationToken(
            project_id=project_id,
            token_index=idx,
            is_beginning_token=idx == token_start_index,
        )
        for idx in range(token_start_index, token_end_index)
    ]


def create_gold_classification_association(
    rlas: List[Any], current_user_id: str, with_commit: bool = False
) -> None:
    for rla in rlas:
        general.expunge(rla)
        make_transient(rla)
        rla.id = None
        rla.created_at = None
        rla.is_gold_star = True
        rla.created_by = current_user_id
        general.add(rla, with_commit)


def create_gold_extraction_association(
    rlas: List[Any], current_user_id: str, with_commit: bool = False
) -> None:
    rla_ids = [rla.id for rla in rlas]
    rla_ids_lookup = {}
    for rla in rlas:
        id = rla.id
        general.expunge(rla)
        make_transient(rla)
        rla.id = None
        rla.created_at = None
        rla.is_gold_star = True
        rla.created_by = current_user_id
        general.add(rla)
        rla_ids_lookup[id] = rla.id

    tokens = session.query(RecordLabelAssociationToken).filter(
        RecordLabelAssociationToken.record_label_association_id.in_(rla_ids)
    )
    for token in tokens:
        token_rla_id = token.record_label_association_id
        general.expunge(token)
        make_transient(token)
        token.id = None
        token.record_label_association_id = rla_ids_lookup[token_rla_id]
        general.add(token)
    general.flush_or_commit(with_commit)


def create_record_label_associations(
    records: List[Record],
    labels_data: List[Dict[str, Any]],
    project_id: str,
    user_id: str,
    with_commit: bool = False,
) -> None:
    joined_labeling_tasks_and_labels = (
        labeling_task_label.get_label_ids_by_task_and_label_name(project_id)
    )
    for record, label_data_entry in zip(records, labels_data):
        rlas = [
            RecordLabelAssociation(
                project_id=project_id,
                record_id=record.id,
                labeling_task_label_id=joined_labeling_tasks_and_labels.get(
                    label_task_name,
                ).get(label_name),
                source_type=enums.LabelSource.MANUAL.value,
                return_type=enums.InformationSourceReturnType.RETURN.value,
                created_by=user_id,
            )
            for label_task_name, label_name in label_data_entry.items()
        ]
        general.add_all(rlas, with_commit)


def update_is_relevant_manual_label(
    project_id: str,
    labeling_task_id: str,
    record_id: str,
    with_commit: bool = False,
) -> None:
    query = __get_base_query_valid_labels_manual_for_update(
        project_id, labeling_task_id, record_id
    )
    query += f"""    
    UPDATE record_label_association rlaOri
        SET is_valid_manual_label = CASE WHEN vri.rla_id IS NULL THEN FALSE ELSE TRUE END
    FROM record_label_association rla
    INNER JOIN labeling_task_label ltl
        ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id
    LEFT JOIN valid_rla_ids vri
        ON rla.id = vri.rla_id
    WHERE rlaOri.id = rla.id
    AND rla.source_type = '{enums.LabelSource.MANUAL.value}'
    AND rlaOri.source_type = '{enums.LabelSource.MANUAL.value}'
    AND ltl.labeling_task_id = '{labeling_task_id}'
    AND rla.record_id = '{record_id}'
    AND rlaOri.record_id = '{record_id}'
    AND rla.project_id = '{project_id}' 
    AND rlaOri.project_id = '{project_id}' 
    AND ltl.project_id = '{project_id}'
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def update_used_information_sources(
    project_id: str, weak_supervision_id: str, with_commit: bool = False
) -> None:
    # set old to null
    session.query(RecordLabelAssociation).filter(
        RecordLabelAssociation.project_id == project_id,
        RecordLabelAssociation.source_type
        == enums.LabelSource.INFORMATION_SOURCE.value,
    ).update({"weak_supervision_id": None})

    # set current to new id -- unfortunatly is orm not able to do that with a join
    general.execute(
        f"""
    UPDATE record_label_association
    SET weak_supervision_id = '{weak_supervision_id}'
    FROM information_source _is
    WHERE record_label_association.project_id = _is.project_id 
    AND record_label_association.source_id = _is.id
    AND _is.is_selected = True
    AND record_label_association.project_id = '{project_id}' 
    AND record_label_association.source_type = '{enums.LabelSource.INFORMATION_SOURCE.value}'
    """
    )
    general.flush_or_commit(with_commit)


def update_record_label_associations(
    user_id: str,
    project_id: str,
    records: List[Record],
    labels_data: List[Dict[str, Any]],
    with_commit: bool = False,
) -> None:
    concatenated_record_and_rla_ids = concatenate_record_and_rla_ids(
        project_id, records, labels_data
    )
    delete_record_label_associations(
        project_id=project_id, record_task_concatenation=concatenated_record_and_rla_ids
    )
    create_record_label_associations(
        records=records, labels_data=labels_data, project_id=project_id, user_id=user_id
    )
    general.flush_or_commit(with_commit)


def update_is_valid_manual_label_for_project(
    project_id: str, with_commit: bool = False
) -> None:
    query = __get_base_query_valid_labels_manual_for_update(project_id)
    query += f"""
    UPDATE record_label_association rlaOri
        SET is_valid_manual_label = CASE WHEN vri.rla_id IS NULL THEN FALSE ELSE TRUE END
    FROM record_label_association rla
    LEFT JOIN valid_rla_ids vri
        ON rla.id = vri.rla_id
    WHERE rlaOri.id = rla.id
    AND rla.source_type = 'MANUAL'
    AND rlaOri.source_type = 'MANUAL'
    AND rla.project_id = '{project_id}'
    AND rlaOri.project_id = '{project_id}'        
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def delete_by_source_id(
    project_id: str, information_source_id: str, with_commit: bool = False
) -> None:
    session.query(RecordLabelAssociation).filter(
        RecordLabelAssociation.project_id == project_id,
        RecordLabelAssociation.source_id == information_source_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_by_source_id_and_record_ids(
    project_id: str,
    information_source_id: str,
    record_ids: List[str],
    with_commit: bool = False,
) -> None:
    session.query(RecordLabelAssociation).filter(
        RecordLabelAssociation.project_id == project_id,
        RecordLabelAssociation.source_id == information_source_id,
        RecordLabelAssociation.record_id.in_(record_ids),
    ).delete()
    general.flush_or_commit(with_commit)


def delete_record_label_associations(
    project_id: str, record_task_concatenation: str, with_commit: bool = False
) -> None:
    if not record_task_concatenation:
        return

    sql = f""" DELETE FROM record_label_association
        WHERE id IN (
            SELECT rla.id
            FROM record_label_association rla 
            INNER JOIN labeling_task_label ltl 
            ON ltl.id  = rla.labeling_task_label_id AND ltl.project_id = '{project_id}'  
            INNER JOIN labeling_task lt 
            ON lt.id = ltl.labeling_task_id AND lt.project_id = '{project_id}' 
            WHERE rla.project_id = '{project_id}'  
            AND rla.record_id::text || lt.id::text IN ({record_task_concatenation}))
        """

    general.execute(sql)
    general.flush_or_commit(with_commit)


def delete(
    project_id: str,
    record_id: str,
    user_id: str,
    label_ids: List[str],
    as_gold_star: Optional[bool] = None,
    source_id: str = None,
    source_type: str = enums.LabelSource.MANUAL.value,
    with_commit: bool = False,
) -> None:
    delete_query = session.query(RecordLabelAssociation).filter(
        RecordLabelAssociation.project_id == project_id,
        RecordLabelAssociation.record_id == record_id,
        RecordLabelAssociation.labeling_task_label_id.in_(label_ids),
        RecordLabelAssociation.source_type == source_type,
    )
    if as_gold_star:
        delete_query = delete_query.filter(RecordLabelAssociation.is_gold_star == True)
    else:
        delete_query = delete_query.filter(
            RecordLabelAssociation.created_by == user_id,
            or_(
                RecordLabelAssociation.is_gold_star == False,
                RecordLabelAssociation.is_gold_star == None,
            ),
        )
    if source_id:
        delete_query = delete_query.filter(
            RecordLabelAssociation.source_id == source_id
        )
    delete_query.delete(synchronize_session="fetch")
    general.flush_or_commit(with_commit)


def delete_by_ids(
    project_id: str,
    record_id: str,
    association_ids: List[str],
    with_commit: bool = False,
) -> None:
    session.query(RecordLabelAssociation).filter(
        RecordLabelAssociation.project_id == project_id,
        RecordLabelAssociation.record_id == record_id,
        RecordLabelAssociation.id.in_(association_ids),
    ).delete()
    general.flush_or_commit(with_commit)


def concatenate_record_and_rla_ids(
    project_id: str, records: List[Record], labels_data: List[Dict[str, Any]]
) -> str:
    task_ids = labeling_task.get_task_name_id_dict(project_id=project_id)
    concatenated_ids = ""
    for record, label_data_entry in zip(records, labels_data):
        for label_task_item in label_data_entry:
            concatenated_ids = (
                f"{concatenated_ids}'{record.id}{task_ids.get(label_task_item)}', "
            )
    return concatenated_ids[:-2]


def check_label_duplication_classification(
    project_id: str, record_id: str, user_id: str, label_ids: List[str]
) -> bool:
    if len(label_ids) == 0:
        return False
    label_id_str = "'" + "', '".join(label_ids) + "'"
    # sleep a bit to ensure requests went through
    time.sleep(0.5)
    query = f"""
    SELECT MAX(id::TEXT)::UUID id_to_keep
    FROM record_label_association rla
    WHERE project_id = '{project_id}' AND record_id = '{record_id}' 
    AND labeling_task_label_id IN ({label_id_str})
    AND created_by = '{user_id}' 
    AND (is_gold_star IS NULL OR is_gold_star = FALSE)
    AND source_type = '{enums.LabelSource.MANUAL.value}'
    HAVING COUNT(*) > 1
    """
    id_to_keep = general.execute_first(query)
    if id_to_keep:
        id_to_keep = id_to_keep[0]
    else:
        return False

    query = f"""
    DELETE FROM record_label_association
    WHERE project_id = '{project_id}' AND record_id = '{record_id}' 
    AND labeling_task_label_id IN ({label_id_str})
    AND created_by = '{user_id}' 
    AND (is_gold_star IS NULL OR is_gold_star = FALSE)
    AND source_type = '{enums.LabelSource.MANUAL.value}'
    AND id != '{id_to_keep}'
    """
    general.execute(query)
    general.commit()
    return True


def is_any_record_manually_labeled(project_id: str):
    query = f"""
    SELECT id
    FROM record_label_association rla
    WHERE project_id = '{project_id}'
    AND source_type = '{enums.LabelSource.MANUAL.value}'
    LIMIT 1
    """
    value = general.execute_first(query)
    return value is not None


def __get_base_query_valid_labels_manual_for_update(
    project_id: str, labeling_task_id: str = "", record_id: str = ""
) -> str:
    # gold /gold star + all where all agree needs to be found so a bit more complicated
    # with SELECT * FROM valid_record_tasks all can be grabbed
    labeling_task_add = ""
    if labeling_task_id:
        labeling_task_add = f"AND ltl.labeling_task_id = '{labeling_task_id}'"
    record_id_add = ""
    if record_id:
        record_id_add = f"AND rla.record_id = '{record_id}'"
    query = f"""
    WITH uncertain_tasks AS (
        {__get_uncertain_tasks_query(project_id,labeling_task_add,record_id_add)}
    ), valid_rla_ids AS(
        {__get_valid_rla_ids_query(project_id,labeling_task_add,record_id_add)}
    )
    """
    return query


def __get_valid_rla_ids_query(
    project_id: str, labeling_task_add: str, record_id_add: str
) -> str:
    return f"""
    {__get_gold_records_classification_query(project_id,labeling_task_add,record_id_add)}    
    UNION
    {__get_gold_records_extraction_query(project_id,labeling_task_add,record_id_add)}    
    UNION
    {__get_gold_star_query(project_id,labeling_task_add,record_id_add)}
    UNION
    {__get_same_answer_classification_query(project_id,labeling_task_add,record_id_add)}
    UNION
    {__get_same_answer_extraction_query(project_id,labeling_task_add,record_id_add)}
    """


def __get_uncertain_tasks_query(
    project_id: str, labeling_task_add: str, record_id_add: str
) -> str:
    # all record_ids & tasks with more than one creation user
    return f"""
        SELECT record_id, labeling_task_id,COUNT(*) opinions,CASE SUM(has_gold_star) WHEN 0 THEN FALSE ELSE TRUE END has_gold_star
        FROM ( 
            SELECT rla.record_id, ltl.labeling_task_id, rla.created_by, COUNT(*),COUNT(rla.is_gold_star) has_gold_star
            FROM record_label_association rla
            INNER JOIN labeling_task_label ltl
                ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id
            WHERE rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}' 
                AND rla.source_type = '{enums.LabelSource.MANUAL.value}'
                {labeling_task_add}
                {record_id_add}
            GROUP BY rla.record_id, ltl.labeling_task_id, rla.created_by ) i
        GROUP BY record_id, labeling_task_id
        HAVING COUNT(*) > 1    
    """


def __get_gold_records_classification_query(
    project_id: str, labeling_task_add: str, record_id_add: str
) -> str:
    # everything from classification with only one creation user
    return f"""
        /*Gold Record (only one opinion) - classification*/
        SELECT rla_id
        FROM (
            SELECT DISTINCT ON (rla.record_id, rla.created_by, ltl.labeling_task_id) rla.id rla_id
            FROM record_label_association rla
            INNER JOIN labeling_task_label ltl
                ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id 
                AND rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}' 
            LEFT JOIN uncertain_tasks ut
                ON ut.record_id = rla.record_id AND ltl.labeling_task_id = ut.labeling_task_id
            WHERE ut.record_id IS NULL 
                AND rla.source_type = '{enums.LabelSource.MANUAL.value}' 
                AND rla.return_type = '{enums.InformationSourceReturnType.RETURN.value}'
                {labeling_task_add}
                {record_id_add} ) x
    """


def __get_gold_records_extraction_query(
    project_id: str, labeling_task_add: str, record_id_add: str
) -> str:
    # everything from extraction with only one creation user
    return f"""
    /*Gold Record (only one opinion) - extraction*/
    SELECT unnest(rla_ids) rla_id
    FROM (     
        SELECT rla.record_id,ltl.labeling_task_id,rla.created_by,COUNT(*),array_agg(rlat.token_index || '-' || rla.labeling_task_label_id::TEXT ORDER BY rlat.token_index) t_index, array_agg(rla.id) rla_ids
        FROM record_label_association rla
        INNER JOIN labeling_task_label ltl
            ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id
            AND rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}'
        LEFT JOIN uncertain_tasks ut
            ON ut.record_id = rla.record_id AND ltl.labeling_task_id = ut.labeling_task_id
        INNER JOIN record_label_association_token rlat
            ON rla.id = rlat.record_label_association_id
        WHERE ut.record_id IS NULL AND rla.source_type = '{enums.LabelSource.MANUAL.value}' 
        AND rla.return_type = '{enums.InformationSourceReturnType.YIELD.value}'
        {labeling_task_add}
        {record_id_add}
        GROUP BY rla.record_id,ltl.labeling_task_id,rla.created_by )x    
    """


def __get_gold_star_query(
    project_id: str, labeling_task_add: str, record_id_add: str
) -> str:
    # everything solved by selecting the Gold Star selection
    return f"""
    /*Gold Star (uncertainty solved by selection)*/
    SELECT rla.id rla_id
    FROM record_label_association rla
    INNER JOIN labeling_task_label ltl
        ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id 
        AND rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}' 
    LEFT JOIN uncertain_tasks ut
        ON ut.record_id = rla.record_id AND ltl.labeling_task_id = ut.labeling_task_id
    WHERE ut.has_gold_star AND rla.is_gold_star AND rla.source_type = '{enums.LabelSource.MANUAL.value}' 
        {labeling_task_add}
        {record_id_add}
    """


def __get_same_answer_classification_query(
    project_id: str, labeling_task_add: str, record_id_add: str
) -> str:
    # everything where all selected the same label - classification
    return f"""
    /*classification problem records all with same label*/
    SELECT unnest(rla_ids) rla_id
    FROM (
        SELECT record_id,labeling_task_id, array_agg(rla_ids[1]) rla_ids
        FROM (
            SELECT rla.record_id,ltl.labeling_task_id,rla.labeling_task_label_id, array_agg(rla.id) rla_ids,COUNT(*)
            FROM record_label_association rla
            INNER JOIN labeling_task_label ltl
                ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id 
                AND rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}' 
            INNER JOIN uncertain_tasks ut
                ON ut.record_id = rla.record_id AND ltl.labeling_task_id = ut.labeling_task_id
            WHERE ut.has_gold_star = FALSE AND rla.source_type = '{enums.LabelSource.MANUAL.value}' AND rla.return_type = '{enums.InformationSourceReturnType.RETURN.value}' 
                {labeling_task_add}
                {record_id_add}
            GROUP BY rla.record_id,ltl.labeling_task_id,rla.labeling_task_label_id ) i
        GROUP BY record_id,labeling_task_id
        HAVING COUNT(*) = 1) x    
    """


def __get_same_answer_extraction_query(
    project_id: str, labeling_task_add: str, record_id_add: str
) -> str:
    # everything where all selected the same label - extraction
    return f"""
    /*problem records extraction all with same label & token index combination are equal = ok */
    SELECT unnest(rla_ids) rla_id
    FROM (
        SELECT record_id,labeling_task_id,MAX(rla_ids) rla_ids
        FROM (
            SELECT record_id,labeling_task_id,t_index, COUNT(*),MAX(rla_ids) rla_ids
            FROM (
                SELECT rla.record_id,ltl.labeling_task_id,rla.created_by,COUNT(*),array_agg(rlat.token_index || '-' || rla.labeling_task_label_id::TEXT ORDER BY rlat.token_index) t_index, array_agg(rla.id) rla_ids
                FROM record_label_association rla
                INNER JOIN labeling_task_label ltl
                    ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id 
                    AND rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}'
                INNER JOIN uncertain_tasks ut
                    ON ut.record_id = rla.record_id AND ltl.labeling_task_id = ut.labeling_task_id
                INNER JOIN record_label_association_token rlat
                    ON rla.id = rlat.record_label_association_id
                WHERE ut.has_gold_star = FALSE AND rla.source_type = '{enums.LabelSource.MANUAL.value}' AND rla.return_type = '{enums.InformationSourceReturnType.YIELD.value}' 
                    {labeling_task_add}
                    {record_id_add}
                GROUP BY rla.record_id,ltl.labeling_task_id,rla.created_by)i
            GROUP BY record_id, labeling_task_id,t_index
        )x
        GROUP BY record_id, labeling_task_id
        HAVING COUNT(*) = 1 )x
    """


def get_manual_classifications_for_labeling_task_as_json(
    project_id: str, labeling_task_id: str
) -> List[Dict[Any, Any]]:
    base_query = payload.get_base_query_valid_labels_manual(
        project_id, labeling_task_id
    )
    query = (
        base_query
        + f"""
    SELECT row_to_json(t) json_obj
    FROM (
        SELECT rla.record_id, rla.source_id,rla.source_type, rla.confidence, rla.labeling_task_label_id label_id
        FROM valid_rla_ids vri
        INNER JOIN record_label_association rla
            ON vri.rla_id = rla.id
        WHERE rla.project_id = '{project_id}') t
    ORDER BY t.record_id
    """
    )
    array = [x.json_obj for x in general.execute_all(query)]
    return array


def get_manual_extraction_tokens_for_labeling_task_as_json(
    project_id: str, labeling_task_id: str
) -> List[Dict[Any, Any]]:
    base_query = payload.get_base_query_valid_labels_manual(
        project_id, labeling_task_id
    )
    query = (
        base_query
        + f"""
    SELECT row_to_json(t) json_obj
    FROM (
        SELECT rla.record_id, rla.source_id, rla.source_type, rla.confidence, rla.labeling_task_label_id label_id, rlat.token_index, rlat.is_beginning_token
        FROM valid_rla_ids vri
        INNER JOIN record_label_association rla
            ON vri.rla_id = rla.id
        INNER JOIN record_label_association_token rlat
            ON rla.id = rlat.record_label_association_id
        WHERE rla.project_id = '{project_id}') t
    ORDER BY t.record_id, t.token_index
    """
    )
    array = [x.json_obj for x in general.execute_all(query)]
    return array


def update_user_id_for_sample_project(
    project_id: str, user_id: str, with_commit: bool = False
):
    query = f"""
    SELECT created_by, COUNT(*) c
    FROM record_label_association rla
    WHERE project_id = '{project_id}'
    GROUP BY created_by 
    ORDER BY c DESC
    """
    user_count = general.execute_first(query)
    if not user_count or not user_count.created_by:
        return
    query = f"""
    UPDATE record_label_association
    SET created_by = '{user_id}'
    WHERE project_id = '{project_id}' AND created_by = '{user_count.created_by}'
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def get_percentage_of_labeled_records_for_slice(
    project_id: str, annotator_id: str, slice_id: str, labeling_task_id: str
) -> float:
    query = get_percentage_of_labeled_records_for_slice_query(
        project_id, annotator_id, slice_id, labeling_task_id
    )
    value = general.execute_first(query)
    if not value:
        return -1
    return value[0]


def get_percentage_of_labeled_records_for_slice_query(
    project_id: str, annotator_id: str, slice_id: str, labeling_task_id: str
) -> str:
    return f"""
    WITH label_data AS (
        SELECT label_check.has_labels,COUNT(*) c
        FROM record r
        INNER JOIN data_slice_record_association dsra
            ON r.id = dsra.record_id AND r.project_id = dsra.project_id AND dsra.data_slice_id = '{slice_id}'
        INNER JOIN ( 
            SELECT r.id record_id, CASE WHEN x.id IS NULL THEN 0 ELSE 1 END has_labels
            FROM record r
            LEFT JOIN LATERAL(
                SELECT rla.id 
                FROM record_label_association rla
                INNER JOIN labeling_task_label ltl
            	    ON rla.project_id = ltl.project_id AND rla.labeling_task_label_id = ltl.id AND ltl.labeling_task_id = '{labeling_task_id}'
                WHERE r.id = rla.record_id
                AND r.project_id = rla.project_id
                AND rla.source_type = 'INFORMATION_SOURCE'
                AND rla.created_by = '{annotator_id}' 
                LIMIT 1
            )x ON TRUE
            WHERE r.project_id = '{project_id}'
        ) label_check
            ON r.id = label_check.record_id
        WHERE r.project_id = '{project_id}'
        GROUP BY label_check.has_labels )

    SELECT has_labels.c / (has_labels.c + has_no_labels.c) percentage
    FROM (
        SELECT C::FLOAT
        FROM label_data
        WHERE has_labels = 1
        UNION ALL
        SELECT 0
        LIMIT 1
    ) has_labels,
    (   SELECT c::FLOAT
        FROM label_data
        WHERE has_labels = 0			
        UNION ALL
        SELECT 0
        LIMIT 1
    )has_no_labels
   
    """
