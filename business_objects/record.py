from __future__ import with_statement
from typing import List, Dict, Any, Optional, Tuple, Iterable
from sqlalchemy import cast, Text
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import bindparam
from sqlalchemy import update

from . import attribute, general
from .. import models, enums
from ..models import (
    Record,
    RecordLabelAssociation,
    LabelingTaskLabel,
    RecordAttributeTokenStatistics,
    Attribute,
    RecordTokenized,
)
from ..session import session
from ..util import prevent_sql_injection


def get(project_id: str, record_id: str) -> Record:
    return (
        session.query(Record)
        .filter(Record.project_id == project_id, Record.id == record_id)
        .first()
    )


def get_one(project_id: str) -> Record:
    return session.query(Record).filter(Record.project_id == project_id).first()


def get_by_record_ids(project_id: str, record_ids: Iterable[str]) -> List[Record]:
    return (
        session.query(Record)
        .filter(Record.project_id == project_id, Record.id.in_(record_ids))
        .all()
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


def get_all_ids(project_id: str) -> List[str]:
    record_ids = (
        session.query(cast(Record.id, Text))
        .filter(Record.project_id == project_id)
        .all()
    )
    return [record_id for record_id, in record_ids]


def get_sample_data_of(
    project_id: str,
    attribute_name: str,
    limit: int = 10,
    add_condition: Optional[str] = None,
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    attribute_name = prevent_sql_injection(
        attribute_name, isinstance(attribute_name, str)
    )
    limit = prevent_sql_injection(limit, isinstance(limit, int))
    # any return since json value is return (-> vs ->>) so casting isn't necessary
    where_add = ""
    if add_condition:
        where_add = "AND " + add_condition

    query = f"""
    SELECT r.data->'{attribute_name}'
    FROM record r
    WHERE r.project_id = '{project_id}'
    {where_add}
    ORDER BY RANDOM()
    LIMIT {limit}
    """
    return [row[0] for row in general.execute_all(query)]


def get_max_running_id(project_id: str) -> int:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    SELECT max(CAST(DATA->>'running_id' AS INTEGER)) max_running_id
    FROM record
    WHERE project_id = '{project_id}'
    """
    return general.execute_first(query)[0]


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


def count_records_without_tokenization(project_id: str) -> int:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    SELECT count(r.id) c
    FROM ({get_records_without_tokenization(project_id, 0, True)}) r
    """
    return general.execute_first(query).c


def count_records_without_manual_label(project_id: str) -> int:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    SELECT COUNT(r.id) c
    FROM record r
    LEFT JOIN (
        SELECT record_id
        FROM record_label_association rla
        WHERE rla.project_id = '{project_id}' AND rla.source_type = '{enums.LabelSource.MANUAL.value}'
        GROUP BY record_id ) rla
        ON r.id = rla.record_id
    WHERE r.project_id = '{project_id}' AND rla.record_id IS NULL
    """
    return general.execute_first(query).c


def get_attribute_data_with_doc_bins_of_records(
    project_id: str, attribute_name: str
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    attribute_name = prevent_sql_injection(
        attribute_name, isinstance(attribute_name, str)
    )
    query = f"""
    SELECT 
        rt.id, 
        r."data" ->> '{attribute_name}' attribute_data, 
        rt.bytes
    FROM record r
    INNER JOIN record_tokenized rt
        ON  r.project_id = rt.project_id AND r.id = rt.record_id
    WHERE r.project_id = '{project_id}'
    """
    return general.execute_all(query)


def update_bytes_of_record_tokenized(
    values: List[Dict[str, Any]], project_id: str
) -> None:
    query = (
        update(RecordTokenized)
        .where(
            RecordTokenized.id == bindparam("_id"),
            RecordTokenized.project_id == project_id,
        )
        .values({"bytes": bindparam("bytes")})
    )
    general.execute(query, values)
    general.flush()


def update_columns_of_tokenized_records(rt_ids: str, attribute_name: str) -> None:
    # rt_ids = prevent_sql_injection(rt_ids, isinstance(rt_ids, str)) # excluded since already prepared beforehand
    attribute_name = prevent_sql_injection(
        attribute_name, isinstance(attribute_name, str)
    )
    query = f"""
    UPDATE record_tokenized 
    SET columns = array_append(columns, '{attribute_name}')
    WHERE id IN {rt_ids}
    """
    general.execute(query)
    general.flush()


def get_missing_rats_records(
    project_id: str, attribute_id: str, limit: int = 0
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    attribute_id = prevent_sql_injection(attribute_id, isinstance(attribute_id, str))
    limit = prevent_sql_injection(limit, isinstance(limit, int))
    attribute_add = f"AND att.data_type = '{enums.TokenizerTask.TYPE_TEXT.value}'"
    attribute_add += f"AND att.state IN ('{enums.AttributeState.UPLOADED.value}', '{enums.AttributeState.USABLE.value}', '{enums.AttributeState.RUNNING.value}')"
    if attribute_id:
        attribute_add += f" AND att.id = '{attribute_id}'"
    query = f"""
    SELECT r.id record_id, array_agg(att.id) attribute_ids, rt.bytes bytes, rt.columns "columns"
    FROM record r
    INNER JOIN attribute att
        ON r.project_id = att.project_id {attribute_add}
    LEFT JOIN record_attribute_token_statistics rats
        ON r.id = rats.record_id 
        AND att.id = rats.attribute_id
    LEFT JOIN record_tokenized rt
        ON rt.record_id = r.id
        AND rt.project_id = r.project_id
    WHERE r.project_id = '{project_id}' 
    AND rats.id IS NULL
    GROUP BY r.id, rt.bytes, rt.columns
    """
    if limit > 0:
        query += f"LIMIT {limit} "
    return general.execute_all(query)


def get_records_without_tokenization(
    project_id: str, limit: int = 0, query_only: bool = False
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    limit = prevent_sql_injection(limit, isinstance(limit, int))
    query = f"""
    SELECT r.id, r.data, rt."columns"
    FROM record r
    LEFT JOIN record_tokenized rt
        ON r.id = rt.record_id
        AND r.project_id = rt.project_id
        AND rt.project_id = '{project_id}'
    WHERE r.project_id = '{project_id}'
    AND rt.id IS NULL

    """
    if query_only:
        return query
    if limit > 0:
        query += f"LIMIT {limit} "
    return general.execute_all(query)


def get_missing_tokenized_records(
    project_id: str,
    attribute_names_string: str,
    limit: int = 0,
    query_only: bool = False,
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    # attribute_names_string = prevent_sql_injection(attribute_names_string, isinstance(attribute_names_string, str)) # excluded since prepared string not single value
    limit = prevent_sql_injection(limit, isinstance(limit, int))
    query = f"""
    SELECT r.id, r.data, rt."columns"
    FROM record r
    LEFT JOIN record_tokenized rt
        ON r.id = rt.record_id
        AND r.project_id = rt.project_id
        AND rt.project_id = '{project_id}'
    WHERE r.project_id = '{project_id}'
    AND (
        NOT rt."columns" @> '{attribute_names_string}'
        OR rt."columns" IS NULL
    )
    """
    if query_only:
        return query
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


def get_count_all_records(project_id: str) -> int:
    return (
        session.query(models.Record)
        .filter(
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    n = prevent_sql_injection(n, isinstance(n, int))
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
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


def get_record_id_groups(project_id: str, group_size: int = 20) -> List[List[str]]:
    if group_size <= 0:
        return None

    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    group_size = prevent_sql_injection(group_size, isinstance(group_size, int))
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    record_ids = [prevent_sql_injection(r, isinstance(r, str)) for r in record_ids]
    attribute_name = prevent_sql_injection(
        attribute_name, isinstance(attribute_name, str)
    )
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


def get_full_record_data_for_id_group(
    project_id: str, record_ids: List[str]
) -> Dict[str, str]:
    if len(record_ids) == 0:
        return []
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    record_ids = [prevent_sql_injection(r, isinstance(r, str)) for r in record_ids]
    record_where = " id IN ('" + "','".join(record_ids) + "')"
    query = f"""
    SELECT id::TEXT, data::JSON
    FROM record
    WHERE project_id = '{project_id}' AND {record_where}
    """
    data = general.execute_all(query)
    return {row[0]: row[1] for row in data} if data else None


def get_attribute_data(
    project_id: str, attribute_name: str
) -> Tuple[List[str], List[str]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    attribute_name = prevent_sql_injection(
        attribute_name, isinstance(attribute_name, str)
    )
    query = None
    order = __get_order_by(project_id)
    if attribute.get_by_name(project_id, attribute_name).data_type == "EMBEDDING_LIST":
        query = f"""
        SELECT id::TEXT || '@' || sub_key id, att AS "{attribute_name}"
        FROM (
            SELECT id, value as att, ordinality - 1 as sub_key
            FROM record
            cross join json_array_elements_text((data::JSON->'{attribute_name}')) with ordinality
            WHERE project_id = '{project_id}'
            {order} 
        )x """
    else:
        query = f"""
        SELECT id::TEXT, data::JSON->'{attribute_name}' AS "{attribute_name}"
        FROM record
        WHERE project_id = '{project_id}'
        {order}
        """
    result = general.execute_all(query)
    record_ids, attribute_values = list(zip(*result))
    return record_ids, attribute_values


def count(project_id: str) -> int:
    return session.query(Record).filter(Record.project_id == project_id).count()


def count_attribute_list_entries(project_id: str, attribute_name: str) -> int:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    attribute_name = prevent_sql_injection(
        attribute_name, isinstance(attribute_name, str)
    )
    query = f"""
    SELECT sum(json_array_length(r.data->'{attribute_name}'))
    FROM record  r
    WHERE project_id = '{project_id}'
    """
    value = general.execute_first(query)
    if not value or not value[0]:
        return 0
    return value[0]


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
def count_missing_rats_records(
    project_id: str, attribute_id: Optional[str] = None
) -> int:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    attribute_id = prevent_sql_injection(attribute_id, isinstance(attribute_id, str))
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    SELECT COUNT(*)
    FROM (
        {get_records_without_tokenization(project_id, None, query_only = True)}
    ) record_query
    """
    return general.execute_first(query)[0]


def count_tokenized_records(project_id: str) -> int:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
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
    changed = 0
    for record_id, attribute_value in calculated_attributes.items():
        record_item = get(project_id=project_id, record_id=record_id)
        if not record_item:
            # this can happen if an record was deleted or the tokenizer file isn't up to date
            continue
        record_item.data[attribute_item.name] = attribute_value
        flag_modified(record_item, "data")
        if changed > 1000:
            changed = 0
            general.flush_or_commit(with_commit)
        changed += 1
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


def delete_duplicated_rats(with_commit: bool = False) -> None:
    # no project so run for all to prevent expensive join with record table
    query = """
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    record_id = prevent_sql_injection(record_id, isinstance(record_id, str))
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


def get_tokenized_record_from_db(
    project_id: str, record_id: str
) -> models.RecordTokenized:
    return (
        session.query(models.RecordTokenized)
        .filter(
            models.RecordTokenized.project_id == project_id,
            models.RecordTokenized.record_id == record_id,
        )
        .first()
    )


def get_tokenized_records_from_db(
    project_id: str, record_ids: List[str]
) -> List[models.RecordTokenized]:
    return (
        session.query(models.RecordTokenized)
        .filter(
            models.RecordTokenized.project_id == project_id,
            models.RecordTokenized.record_id.in_(record_ids),
        )
        .all()
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

        r_id = attribute.get_running_id_name(project_id)
        if x.data_type == "INTEGER" and x.name == r_id:
            # only running_id gets cast as other aren't sure to be integers (e.g. empty fields)
            tmp = f"({tmp})::INTEGER"
        order += tmp

    if order != "":
        order = "ORDER BY " + order
    return order


def get_first_no_text_column(project_id: str, record_id: str) -> str:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    record_id = prevent_sql_injection(record_id, isinstance(record_id, str))
    query = f"""
    SELECT '''' || x.name || ': ' || (r.data ->>x.name) || '''' AS name_col
    FROM record r,
    (
        SELECT a.name
        FROM attribute a 
        WHERE data_type NOT IN('{enums.DataTypes.TEXT.value}' , '{enums.DataTypes.CATEGORY.value}')
            AND a.state IN ('{enums.AttributeState.AUTOMATICALLY_CREATED.value}','{enums.AttributeState.UPLOADED.value}','{enums.AttributeState.USABLE.value}')
            AND a.project_id = '{project_id}'
        ORDER BY a.relative_position
        LIMIT 1 
    )x
    WHERE r.project_id = '{project_id}' AND r.id = '{record_id}'
    """
    return general.execute_first(query)[0]
