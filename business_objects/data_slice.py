from datetime import datetime
from typing import Any, List, Dict, Optional

from ..business_objects import general
from ..models import DataSlice, DataSliceRecordAssociation
from ..session import session
from .. import enums


def get(
    project_id: str, data_slice_id: str, only_static: Optional[bool] = False
) -> DataSlice:
    query = session.query(DataSlice).filter(
        DataSlice.project_id == project_id,
        DataSlice.id == data_slice_id,
    )
    if only_static:
        query.filter(DataSlice.static == True)
    return query.first()


def get_all(project_id: str) -> List[DataSlice]:
    return (
        session.query(DataSlice)
        .filter(
            DataSlice.project_id == project_id,
        )
        .order_by(DataSlice.name)
        .all()
    )


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
