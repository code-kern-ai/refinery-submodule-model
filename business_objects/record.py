from __future__ import with_statement
from typing import List, Dict, Any, Tuple
from sqlalchemy import cast, Text
from sqlalchemy.orm.attributes import flag_modified

from . import attribute, general
from .. import models, enums
from ..models import (
    Record,
    RecordLabelAssociation,
    LabelingTaskLabel,
    RecordAttributeTokenStatistics,
    Attribute,
)
from ..session import session


def get(project_id: str, record_id: str) -> Record:
    return (
        session.query(Record)
        .filter(Record.project_id == project_id, Record.id == record_id)
        .first()
    )


def get_without_project_id(record_id: str) -> Record:
    """
    Attention: instead of this method use get(project_id, record_id),
    this is a temporary solution and should be fixed by adding
    the project id to every record query/mutation
    """
    return session.query(Record).filter(Record.id == record_id).first()


def get_all(project_id: str) -> List[Record]:
    return session.query(Record).filter(Record.project_id == project_id).all()


def get_existing_records_by_composite_key(
    project_id: str,
    records_data: List[Dict[str, Any]],
    primary_keys: List[Attribute],
    category: str,
) -> Dict[str, Record]:
    query = (
        session.query(Record)
        .filter(Record.project_id == project_id)
        .filter(Record.category == category)
    )

    for primary_key in primary_keys:
        query = query.filter(
            Record.data[primary_key.name]
            .as_string()
            .in_([str(record_item[primary_key.name]) for record_item in records_data])
        )
    existing_records = query.all()

    return {
        __infer_concatenated_primary_key_of_record(
            record=record, primary_keys=primary_keys
        ): record
        for record in existing_records
    }


def get_ids_by_keys(keys: Dict[str, Any]) -> List[Tuple[str, str]]:
    return session.query(cast(Record.id, Text)).filter(Record.id.in_(keys.keys())).all()


def get_ids_of_manual_records_by_labeling_task(
    project_id: str, labeling_task_id: str
) -> List[str]:
    return (
        session.query(RecordLabelAssociation.record_id)
        .filter(
            RecordLabelAssociation.source_type == enums.LabelSource.MANUAL.value,
            RecordLabelAssociation.project_id == project_id,
            RecordLabelAssociation.labeling_task_label_id == LabelingTaskLabel.id,
            LabelingTaskLabel.labeling_task_id == labeling_task_id,
        )
        .group_by(RecordLabelAssociation.record_id)
        .all()
    )


def get_missing_rats_records(
    project_id: str, limit: int, attribute_id: str
) -> List[Any]:
    attribute_add = f"AND att.data_type = '{enums.TokenizerTask.TYPE_TEXT.value}'"
    attribute_add += f"AND att.state IN ('{enums.AttributeState.UPLOADED.value}', '{enums.AttributeState.USABLE.value}', '{enums.AttributeState.RUNNING.value}')"
    if attribute_id:
        attribute_add += f" AND att.id = '{attribute_id}'"
    query = f"""
    SELECT r.id record_id, array_agg(att.id) attribute_ids
    FROM record r
    INNER JOIN attribute att
        ON r.project_id = att.project_id {attribute_add}
    LEFT JOIN record_attribute_token_statistics rats
        ON r.id = rats.record_id 
        AND att.id = rats.attribute_id
    WHERE r.project_id = '{project_id}' 
    AND rats.id IS NULL
    GROUP BY r.id
    """
    if limit > 0:
        query += f"LIMIT {limit} "
    return general.execute_all(query)


def get_missing_tokenized_records(project_id: str, limit: int) -> List[Any]:
    query = f"""
    SELECT r.id, r.data
    FROM record r
    LEFT JOIN record_tokenized rt
        ON r.id = rt.record_id 
        AND r.project_id = rt.project_id 
        AND rt.project_id = '{project_id}'
    WHERE r.project_id = '{project_id}' 
    AND rt.id IS NULL    
    """
    if limit > 0:
        query += f"LIMIT {limit} "
    return general.execute_all(query)


def get_count_scale_uploaded(project_id: str) -> int:
    return (
        session.query(models.Record)
        .filter(
            models.Record.category == enums.RecordCategory.SCALE.value,
            models.Record.project_id == project_id,
        )
        .count()
    )


def get_count_test_uploaded(project_id: str) -> int:
    return (
        session.query(models.Record)
        .filter(
            models.Record.category == enums.RecordCategory.TEST.value,
            models.Record.project_id == project_id,
        )
        .count()
    )


def get_token_statistics_by_project_id(project_id: str) -> List[Any]:
    query = f"""
        SELECT
            statistics.id,
            statistics.record_id,
            statistics.attribute_id,
            statistics.num_token
        FROM
            record_attribute_token_statistics as statistics
        INNER JOIN
            attribute
        ON
            statistics.attribute_id=attribute.id
        WHERE
            attribute.project_id='{project_id}'
        ;
        """
    return general.execute_all(query)


def get_attribute_calculation_sample_records(project_id: str, n: int = 10) -> List[Any]:
    query = f"""
        SELECT record.id::TEXT, record."data"
        FROM record
        INNER JOIN record_tokenized rt
            ON record.id = rt.record_id AND record.project_id = rt.project_id
        WHERE record.project_id='{project_id}' AND record.category = '{enums.RecordCategory.SCALE.value}'
        ORDER BY RANDOM()
        LIMIT {n}
        """
    return general.execute_all(query)


def get_missing_columns_str(project_id: str) -> str:
    query = f"""
    SELECT att.name
    FROM attribute att
    LEFT JOIN (
        SELECT unnest(columns) col, project_id
        FROM (
            SELECT columns, project_id
            FROM record_tokenized rt
            WHERE rt.project_id = '{project_id}'
            LIMIT 1 
        ) i
    ) used_attributes
        ON att.project_id = used_attributes.project_id AND att.name = used_attributes.col
    WHERE att.project_id = '{project_id}' AND used_attributes.project_id IS NULL
    AND att.state IN ('{enums.AttributeState.UPLOADED.value}','{enums.AttributeState.USABLE.value}','{enums.AttributeState.AUTOMATICALLY_CREATED.value}')
    """
    missing_columns = general.execute_all(query)
    if not missing_columns:
        return ""
    return ",\n".join([f"'{k[0]}',r.data->'{k[0]}'" for k in missing_columns])


def get_zero_shot_n_random_records(
    project_id: str, attribute_name: str, n: int = 10
) -> List[Any]:
    sql = f"""
        SELECT r.id, r."data", r."data" ->> '{attribute_name}' "text"
        FROM record r
        WHERE project_id = '{project_id}' AND r.category = '{enums.RecordCategory.SCALE.value}'
        ORDER BY RANDOM()
        LIMIT {n}
        """
    return general.execute_all(sql)


def get_record_id_groups(project_id: str, group_size: int = 20) -> List[List[str]]:
    if group_size <= 0:
        return None
    query = f"""
    SELECT array_agg(id) record_ids
    FROM (
        SELECT id::TEXT, FLOOR((ROW_NUMBER() OVER())/{group_size}) gn
        FROM record r
        WHERE r.project_id = '{project_id}'
    )x
    GROUP BY gn
    """
    groups = general.execute_all(query)
    return [g[0] for g in groups] if groups else None


def get_record_data_for_id_group(
    project_id: str, record_ids: List[str], attribute_name: str
) -> Dict[str, str]:
    record_where = " id IN ('" + "','".join(record_ids) + "')"
    query = f"""
    SELECT id::TEXT, data::JSON->'{attribute_name}' AS "{attribute_name}"
    FROM record
    WHERE project_id = '{project_id}' AND {record_where}
    AND data::JSON->'{attribute_name}' IS NOT NULL
    AND LENGTH((data::JSON->'{attribute_name}')::TEXT) > 5
    """
    data = general.execute_all(query)
    return {row[0]: row[1] for row in data} if data else None


def get_attribute_data(
    project_id: str, attribute_name: str
) -> Tuple[List[str], List[str]]:
    order = __get_order_by(project_id)
    query = f"""
    SELECT id, data::JSON->'{attribute_name}' AS "{attribute_name}"
    FROM record
    WHERE project_id = '{project_id}'
    {order}
    """
    result = general.execute_all(query)
    record_ids, attribute_values = list(zip(*result))
    return record_ids, attribute_values


def count(project_id: str) -> int:
    return session.query(Record).filter(Record.project_id == project_id).count()


def count_by_project_and_source(
    project_id: str, record_category: str, label_source: str
) -> int:
    return (
        session.query(models.Record)
        .filter(
            models.Record.category == record_category,
            models.Record.project_id == project_id,
            models.Record.record_label_associations.any(
                models.RecordLabelAssociation.source_type == label_source
            ),
        )
        .count()
    )


# rats = record_attribute_token_statistics
def count_missing_rats_records(project_id: str, attribute_id: str) -> int:
    attribute_add = f"AND att.data_type = '{enums.TokenizerTask.TYPE_TEXT.value}'"
    attribute_add += f"AND att.state IN ('{enums.AttributeState.UPLOADED.value}', '{enums.AttributeState.USABLE.value}', '{enums.AttributeState.RUNNING.value}')"
    if attribute_id:
        attribute_add += f" AND att.id = '{attribute_id}'"
    query = f"""
    SELECT COUNT(*) c
    FROM (
        SELECT r.id
        FROM record r
        INNER JOIN attribute att
            ON r.project_id = att.project_id {attribute_add}
        LEFT JOIN record_attribute_token_statistics rats
            ON r.id = rats.record_id 
            AND att.id = rats.attribute_id
        WHERE r.project_id = '{project_id}' 
        AND rats.id IS NULL
        GROUP BY r.id)x
    """
    result = general.execute_first(query)
    return result.c


def count_missing_tokenized_records(project_id: str) -> int:
    query = f"""
    SELECT COUNT(*) c
    FROM record r
    LEFT JOIN record_tokenized rt
        ON r.id = rt.record_id 
        AND r.project_id = rt.project_id 
        AND rt.project_id = '{project_id}'
    WHERE r.project_id = '{project_id}' 
    AND rt.id IS NULL    
    """
    result = general.execute_first(query)
    return result.c


def count_tokenized_records(project_id: str) -> int:
    query = f"""
    SELECT COUNT(*) c
    FROM record_tokenized rt
    WHERE rt.project_id = '{project_id}'
    """
    result = general.execute_first(query)
    return result.c


def create_or_update_token_statistic(
    project_id: str,
    record_id: str,
    attribute_id: str,
    amount: int,
    with_commit: bool = False,
) -> None:
    # currently project_id isnt in the table -- this will be changed at some point
    tbl_entry = (
        session.query(models.RecordAttributeTokenStatistics)
        .filter(
            models.RecordAttributeTokenStatistics.attribute_id == attribute_id,
            models.RecordAttributeTokenStatistics.record_id == record_id,
        )
        .first()
    )
    if tbl_entry:
        tbl_entry.num_token = amount
    else:
        tbl_entry = models.RecordAttributeTokenStatistics(
            project_id=project_id,
            record_id=record_id,
            attribute_id=attribute_id,
            num_token=amount,
        )
        general.add(tbl_entry)
    general.flush_or_commit(with_commit)


def create(
    project_id: str,
    record_data: Dict,
    category: str,
    with_commit: bool = False,
) -> Record:
    record = Record(
        project_id=project_id,
        data=record_data,
        category=category,
    )
    general.add(record, with_commit)
    return record


def create_records(
    project_id: str,
    records_data: List[Dict[str, Any]],
    category: str,
    with_commit: bool = False,
) -> List[Record]:
    records = [
        Record(
            project_id=project_id,
            data=record_item,
            category=category,
        )
        for record_item in records_data
    ]
    general.add_all(records, with_commit)
    return records


def create_record_attribute_token_statistics(
    project_id: str,
    record_id: str,
    attribute_id: str,
    num_token: int,
    with_commit: bool = False,
) -> RecordAttributeTokenStatistics:
    stats = RecordAttributeTokenStatistics(
        project_id=project_id,
        record_id=record_id,
        attribute_id=attribute_id,
        num_token=num_token,
    )
    general.add(stats, with_commit)
    return stats


def update_records(
    records_data: List[Dict[str, Any]],
    labels_data: List[Dict[str, Any]],
    existing_records_by_key: Dict[str, Record],
    primary_keys: List[Attribute],
) -> Tuple[
    List[Dict[Any, Any]],
    List[Dict[Any, Any]],
    List[Dict[Any, Any]],
    List[Dict[Any, Any]],
]:
    updated_records = []
    labels_data_of_updated_records = []
    records_data_without_db_entries = []
    labels_of_records_without_db_entries = []

    for record_item, label_item in zip(records_data, labels_data):
        record_item_primary_key = __infer_concatenated_primary_key_of_record_item(
            record_item, primary_keys=primary_keys
        )
        if record_item_primary_key in existing_records_by_key:
            record = existing_records_by_key[record_item_primary_key]
            record.data = record_item
            updated_records.append(record)
            labels_data_of_updated_records.append(label_item)
        else:
            records_data_without_db_entries.append(record_item)
            labels_of_records_without_db_entries.append(label_item)

    return (
        records_data_without_db_entries,
        labels_of_records_without_db_entries,
        updated_records,
        labels_data_of_updated_records,
    )


def update_add_user_created_attribute(
    project_id: str,
    attribute_id: str,
    calculated_attributes: Dict[str, str],
    with_commit: bool = False,
) -> None:
    attribute_item = attribute.get(project_id, attribute_id)
    for i, (record_id, attribute_value) in enumerate(calculated_attributes.items()):
        record_item = get(project_id=project_id, record_id=record_id)
        record_item.data[attribute_item.name] = attribute_value
        flag_modified(record_item, "data")
        if (i + 1) % 1000 == 0:
            general.flush_or_commit(with_commit)
    general.flush_or_commit(with_commit)


def delete(project_id: str, record_id: str, with_commit: bool = False) -> None:
    session.delete(
        session.query(Record)
        .filter(Record.project_id == project_id, Record.id == record_id)
        .first()
    )
    general.flush_or_commit(with_commit)


def delete_all(project_id: str, with_commit: bool = False) -> None:
    session.query(Record).filter(Record.project_id == project_id).delete()
    general.flush_or_commit(with_commit)


def delete_user_created_attribute(
    project_id: str, attribute_id: str, with_commit: bool = False
) -> None:
    attribute_item = attribute.get(project_id, attribute_id)

    if not attribute_item.user_created:
        return

    record_items = get_all(project_id=project_id)
    for i, record_item in enumerate(record_items):
        del record_item.data[attribute_item.name]
        flag_modified(record_item, "data")
        if (i + 1) % 1000 == 0:
            general.flush_or_commit(with_commit)
    general.flush_or_commit(with_commit)


def delete_dublicated_rats(with_commit: bool = False) -> None:
    # no project so run for all to prevent expensive join with record table
    query = f"""    
    DELETE FROM record_tokenized rt
    USING (	
        SELECT record_id, attribute_id, (array_agg(id))[1] AS id_to_del
        FROM record_attribute_token_statistics rt
        GROUP BY record_id,attribute_id
        HAVING COUNT(*) >1) AS del_helper
    WHERE rt.id = del_helper.id_to_del       
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def has_byte_data(project_id: str, record_id: str) -> bool:
    query = f"""
    SELECT id
    FROM record_tokenized
    WHERE project_id = '{project_id}' 
    AND record_id = '{record_id}'    
    """
    return general.execute_first(query) is not None


def __infer_concatenated_primary_key_of_record_item(
    record_item: Dict, primary_keys: List[Attribute]
) -> str:
    concatenated_key = ""

    for idx, primary_key in enumerate(primary_keys):
        concatenated_key = f"{concatenated_key}{idx}{record_item[primary_key.name]}"
    return concatenated_key


def __infer_concatenated_primary_key_of_record(
    record: Record, primary_keys: List[Attribute]
) -> str:
    concatenated_key = ""

    for idx, primary_key in enumerate(primary_keys):
        concatenated_key = f"{concatenated_key}{idx}{record.data[primary_key.name]}"
    return concatenated_key


def __get_tokenized_record(project_id: str, record_id: str) -> models.RecordTokenized:
    return (
        session.query(models.RecordTokenized)
        .filter(
            models.RecordTokenized.project_id == project_id,
            models.RecordTokenized.record_id == record_id,
        )
        .first()
    )


def __get_order_by(project_id: str, first_x: int = 3) -> str:
    query = f"""
    SELECT name, data_type
    FROM attribute a
    WHERE a.project_id = '{project_id}'
    ORDER BY a.relative_position
    LIMIT {first_x}
    """
    values = general.execute_all(query)
    order = ""

    for x in values:
        if order != "":
            order += ", "
        tmp = f"data->>'{x.name}'"
        if x.data_type == "INTEGER":
            tmp = f"({tmp})::INTEGER"
        order += tmp

    if order != "":
        order = "ORDER BY " + order
    return order


def get_first_no_text_column(project_id: str, record_id: str) -> str:
    query = f"""
    SELECT '''' || x.name || ': ' || (r.data ->>x.name) || '''' AS name_col
    FROM record r,
    (
        SELECT a.name
        FROM attribute a 
        WHERE data_type NOT IN('{enums.DataTypes.TEXT.value}' , '{enums.DataTypes.CATEGORY.value}')
            AND a.project_id = '{project_id}'
        LIMIT 1 
    )x
    WHERE r.project_id = '{project_id}' AND r.id = '{record_id}'
    """
    return general.execute_first(query)[0]
