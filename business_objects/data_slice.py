from datetime import datetime
import json
from typing import Any, List, Dict, Optional, Tuple

from ..business_objects import general
from ..models import DataSlice, DataSliceRecordAssociation
from ..session import session
from .. import enums
from ..business_objects import information_source


def get(
    project_id: str, data_slice_id: str, only_static: Optional[bool] = False
) -> DataSlice:
    query = session.query(DataSlice).filter(
        DataSlice.project_id == project_id,
        DataSlice.id == data_slice_id,
    )
    if only_static:
        query = query.filter(DataSlice.static == True)
    return query.first()


def get_all(
    project_id: str, slice_type: Optional[enums.SliceTypes] = None
) -> List[DataSlice]:
    query = session.query(DataSlice).filter(
        DataSlice.project_id == project_id,
    )
    if slice_type:
        query = query.filter(DataSlice.slice_type == slice_type.value)
    query = query.order_by(DataSlice.name)
    return query.all()


def get_all_associations(project_id: str) -> List[DataSliceRecordAssociation]:
    return (
        session.query(DataSliceRecordAssociation)
        .filter(DataSliceRecordAssociation.project_id == project_id)
        .all()
    )


def create(
    project_id: str,
    created_by: Optional[str] = None,
    name: Optional[str] = None,
    filter_raw: Dict = None,
    static: Optional[bool] = None,
    count_sql: Optional[str] = None,
    count: Optional[int] = None,
    created_at: Optional[datetime] = None,
    filter_data: Dict = None,
    slice_type: Optional[str] = None,
    info: Optional[str] = None,
    with_commit: bool = False,
) -> DataSlice:
    data_slice = DataSlice(
        project_id=project_id,
        name=name,
        filter_raw=filter_raw,
        static=static,
    )

    if created_by:
        data_slice.created_by = created_by

    if count_sql:
        data_slice.count_sql = count_sql

    if count:
        data_slice.count = count

    if created_at:
        data_slice.created_at = created_at

    if filter_data:
        data_slice.filter_data = filter_data

    if slice_type:
        data_slice.slice_type = slice_type

    if info:
        data_slice.info = info

    general.add(data_slice, with_commit)
    return data_slice


def create_association(
    project_id: str,
    data_slice_id: str,
    record_id: str,
    outlier_score: float = None,
    with_commit: bool = False,
) -> DataSliceRecordAssociation:
    association: DataSliceRecordAssociation = DataSliceRecordAssociation(
        data_slice_id=data_slice_id,
        record_id=record_id,
        project_id=project_id,
    )

    if outlier_score:
        association.outlier_score = outlier_score

    general.add(association, with_commit)
    return association


def delete_associations(
    project_id: str, data_slice_id: str, with_commit: bool = False
) -> None:
    (
        session.query(DataSliceRecordAssociation)
        .filter(
            DataSliceRecordAssociation.project_id == project_id,
            DataSliceRecordAssociation.data_slice_id == data_slice_id,
        )
        .delete()
    )
    general.flush_or_commit(with_commit)


def update_data_slice(
    project_id: str,
    data_slice_id: str,
    static: Optional[bool] = None,
    count: Optional[int] = None,
    count_sql: Optional[str] = None,
    filter_data: Optional[List[Dict[str, Any]]] = None,
    filter_raw: Optional[Dict[str, Any]] = None,
    with_commit: bool = False,
) -> DataSlice:
    data_slice: DataSlice = get(project_id, data_slice_id)
    if static is not None:
        data_slice.static = static
    if count is not None:
        data_slice.count = count
    if count_sql is not None:
        data_slice.count_sql = count_sql
    if filter_data is not None:
        data_slice.filter_data = filter_data
    if filter_raw is not None:
        data_slice.filter_raw = filter_raw

    general.flush_or_commit(with_commit)
    return data_slice


def update_data_slice_record_association_outlier_scores(
    project_id: str,
    data_slice_id: str,
    outlier_ids: List,
    outlier_scores: List,
    with_commit: bool = False,
) -> None:
    sql_strings = [
        "UPDATE data_slice_record_association",
        "SET outlier_score = (CASE record_id",
    ]
    sql_strings.extend(
        [f"WHEN '{id}' THEN {score}" for id, score in zip(outlier_ids, outlier_scores)]
    )
    sql_strings.append("END)")
    sql_strings.append(
        f"WHERE project_id = '{project_id}' AND data_slice_id = '{data_slice_id}'"
    )
    sql_update = "\n".join(sql_strings)
    general.execute(sql_update)
    general.flush_or_commit(with_commit)


def delete(project_id: str, data_slice_id: str, with_commit: bool = False) -> None:
    (
        session.query(DataSlice)
        .filter(
            DataSlice.project_id == project_id,
            DataSlice.id == data_slice_id,
        )
        .delete()
    )
    general.flush_or_commit(with_commit)


def update_slice_type_manual_for_project(
    project_id: str, with_commit: bool = False
) -> None:
    query = __get_updata_slice_type_manual_query()
    if project_id:
        query += f" AND project_id = '{project_id}'"
    else:
        return
    general.execute(query)
    general.flush_or_commit(with_commit)


def update_slice_type_manual_for_all(with_commit: bool = False) -> None:
    query = __get_updata_slice_type_manual_query()
    general.execute(query)
    general.flush_or_commit(with_commit)


def __get_updata_slice_type_manual_query() -> None:
    return f"""
        UPDATE data_slice
        SET slice_type = (
            CASE
                WHEN static = TRUE THEN '{enums.SliceTypes.STATIC_DEFAULT.value}'
                ELSE '{enums.SliceTypes.DYNAMIC_DEFAULT.value}'
            END
        )
        WHERE slice_type IS NULL
        """


def get_record_ids_and_first_unlabeled_pos(
    project_id: str,
    user_id: str,
    data_slice_id: str,
    source_type: enums.LabelSource = enums.LabelSource.MANUAL,
    source_id: Optional[str] = None,
    labeling_task_id: Optional[str] = None,
) -> Tuple[List[str], int]:
    query = __get_record_ids_and_first_unlabeled_pos_query(
        project_id, user_id, data_slice_id, source_type, source_id, labeling_task_id
    )
    values = general.execute_first(query)
    if not values:
        return [], 0
    return values[0], values[1]


def __get_record_ids_and_first_unlabeled_pos_query(
    project_id: str,
    user_id: str,
    data_slice_id: str,
    source_type: enums.LabelSource,
    source_id: str,
    labeling_task_id: str,
):
    source_id_add = ""
    if source_id:
        source_id_add = f"AND rla.source_id = '{source_id}'"
    labeling_task_id_add = ""
    if labeling_task_id:
        labeling_task_id_add = f"""INNER JOIN labeling_task_label ltl
            	ON rla.project_id = ltl.project_id AND rla.labeling_task_label_id = ltl.id AND ltl.labeling_task_id = '{labeling_task_id}'"""
    return f"""
    WITH record_select AS (
    SELECT r.id::TEXT record_id, label_check.has_labels,ROW_NUMBER () OVER(ORDER BY has_labels desc,r.id)-1 rn
    FROM record r
    INNER JOIN data_slice_record_association dsra
        ON r.id = dsra.record_id AND r.project_id = dsra.project_id AND dsra.data_slice_id = '{data_slice_id}'
    INNER JOIN ( 
        SELECT r.id record_id, CASE WHEN x.id IS NULL THEN 0 ELSE 1 END has_labels
        FROM record r
        LEFT JOIN LATERAL(
            SELECT rla.id 
            FROM record_label_association rla
            {labeling_task_id_add}
            WHERE r.id = rla.record_id
            AND r.project_id = rla.project_id
            AND rla.source_type = '{source_type.value}'
            {source_id_add}
            AND rla.created_by = '{user_id}' 
            LIMIT 1
        )x ON TRUE
        WHERE r.project_id = '{project_id}'
    ) label_check
        ON r.id = label_check.record_id
    WHERE r.project_id = '{project_id}')

    SELECT record_ids, rn first_post
    FROM (
        SELECT array_agg(record_id) record_ids
        FROM record_select
    ) x,
    (
        SELECT rn
        FROM (
            SELECT rn
            FROM record_select
            WHERE has_labels =0
            ORDER BY rn
            LIMIT 1) x
        UNION ALL
        SELECT 0 --fallback value if all are labeled
        LIMIT 1
    )y    
    """
