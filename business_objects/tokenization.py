from typing import List, Any, Optional, Iterable


from sqlalchemy import or_

from . import general
from .. import RecordTokenizationTask, enums
from ..models import RecordAttributeTokenStatistics, RecordTokenized, Record
from ..session import session

from ..util import prevent_sql_injection


def get_records_tokenized(
    project_id: str, record_ids: List[str] = None
) -> List[RecordTokenized]:
    query = session.query(RecordTokenized).filter(
        RecordTokenized.project_id == project_id,
    )
    if record_ids:
        query = query.filter(RecordTokenized.record_id.in_(record_ids))
    return query.all()


def get_record_tokenization_task(project_id: str) -> RecordTokenizationTask:
    return (
        session.query(RecordTokenizationTask)
        .filter(
            RecordTokenizationTask.project_id == project_id,
            RecordTokenizationTask.type == enums.TokenizerTask.TYPE_DOC_BIN.value,
        )
        .order_by(RecordTokenizationTask.started_at.desc())
        .first()
    )


def get(project_id: str, task_id: str) -> RecordTokenizationTask:
    return (
        session.query(RecordTokenizationTask)
        .filter(
            RecordTokenizationTask.project_id == project_id,
            RecordTokenizationTask.id == task_id,
        )
        .first()
    )


def get_record_tokenized_entry(project_id: str, record_id: str) -> RecordTokenized:
    return (
        session.query(RecordTokenized)
        .filter(
            RecordTokenized.project_id == project_id,
            RecordTokenized.record_id == record_id,
        )
        .first()
    )


def get_doc_bin_progress(project_id: str) -> str:
    task = (
        session.query(RecordTokenizationTask.progress)
        .filter(
            RecordTokenizationTask.project_id == project_id,
            RecordTokenizationTask.type == enums.TokenizerTask.TYPE_DOC_BIN.value,
        )
        .filter(
            or_(
                RecordTokenizationTask.state
                == enums.TokenizerTask.STATE_IN_PROGRESS.value,
                RecordTokenizationTask.state == enums.TokenizerTask.STATE_CREATED.value,
            )
        )
        .first()
    )

    if task:
        return str(task.progress * 100)
    return ""


def get_doc_bin_table_to_json(
    project_id: str, missing_columns: str, record_ids: List[str] = None
) -> Any:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    # missing_columns = prevent_sql_injection(missing_columns,isinstance(missing_columns,str)) # excluded since already prepared beforehand
    if missing_columns != "":
        missing_columns += ","
    if record_ids:
        record_ids = [
            prevent_sql_injection(record_id, isinstance(record_id, str))
            for record_id in record_ids
        ]
        record_ids = (
            "AND rt.record_id IN ("
            + ",".join([f"'{record_id}'" for record_id in record_ids])
            + ")"
        )
    else:
        record_ids = ""
    query = f"""
        SELECT      
            json_agg(
                json_build_object(
                'record_id', rt.record_id,
                'columns', rt.columns,
                {missing_columns}
                'bytes', rt.bytes)
            )::TEXT AS data
        FROM record_tokenized rt
        INNER JOIN record r
            ON rt.record_id = r.id AND rt.project_id = rt.project_id
        WHERE rt.project_id = '{project_id}'
            AND r.project_id = '{project_id}'
            {record_ids}
    """
    return general.execute_first(query).data


def create_tokenization_task(
    project_id: str,
    user_id: str,
    type: str = enums.TokenizerTask.TYPE_DOC_BIN.value,
    with_commit: bool = False,
    scope: str = enums.RecordTokenizationScope.PROJECT.value,
    attribute_name: Optional[str] = None,
) -> RecordTokenizationTask:
    tbl_entry = RecordTokenizationTask(
        project_id=project_id,
        user_id=user_id,
        state=enums.TokenizerTask.STATE_CREATED.value,
        progress=0,
        type=type,
        scope=scope,
        attribute_name=attribute_name,
    )
    general.add(tbl_entry, with_commit)
    return tbl_entry


def delete_docbins(project_id: str, with_commit: bool = False) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    DELETE FROM record_tokenized
    WHERE project_id = '{project_id}'
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def delete_record_docbins(
    project_id: str, records: List[Record], with_commit: bool = False
) -> None:
    delete_record_docbins_by_id(
        project_id, [record.id for record in records], with_commit
    )


def delete_record_docbins_by_id(
    project_id: str, record_ids: Iterable[str], with_commit: bool = False
) -> None:
    session.query(RecordTokenized).filter(
        RecordTokenized.record_id.in_(record_ids),
        RecordTokenized.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_token_statistics(records: List[Record], with_commit: bool = False) -> None:
    if len(records) == 0:
        return
    project_id = records[0].project_id
    delete_token_statistics_by_id(
        project_id, [record.id for record in records], with_commit
    )


def delete_token_statistics_by_id(
    project_id: str, record_ids: Iterable[str], with_commit: bool = False
) -> None:
    session.query(RecordAttributeTokenStatistics).filter(
        RecordAttributeTokenStatistics.record_id.in_(record_ids),
        RecordAttributeTokenStatistics.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_token_statistics_for_project(
    project_id: str, with_commit: bool = False
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    DELETE FROM record_attribute_token_statistics
    WHERE project_id = '{project_id}'
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def delete_dublicated_tokenization(project_id: str, with_commit: bool = False) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""    
    DELETE FROM record_tokenized rt
    USING (	
        SELECT record_id, (array_agg(id))[1] AS id_to_del
        FROM record_tokenized rt
        WHERE project_id = '{project_id}'
        GROUP BY record_id
        HAVING COUNT(*) >1) AS del_helper
    WHERE rt.id = del_helper.id_to_del       
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def delete_tokenization_tasks(project_id: str, with_commit: bool = False) -> None:
    session.query(RecordTokenizationTask).filter(
        RecordTokenizationTask.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)


def is_doc_bin_creation_running(project_id: str) -> bool:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
        SELECT id
        FROM record_tokenization_task
        WHERE project_id = '{project_id}'
        AND type = '{enums.TokenizerTask.TYPE_DOC_BIN.value}'
        AND state IN ('{enums.TokenizerTask.STATE_IN_PROGRESS.value}', '{enums.TokenizerTask.STATE_CREATED.value}')
        LIMIT 1
    """
    return general.execute_first(query) is not None


def is_doc_bin_creation_running_for_attribute(
    project_id: str, attribute_name: str
) -> bool:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    attribute_name = prevent_sql_injection(
        attribute_name, isinstance(attribute_name, str)
    )
    query = f"""
        SELECT id
        FROM record_tokenization_task
        WHERE project_id = '{project_id}'
        AND type = '{enums.TokenizerTask.TYPE_DOC_BIN.value}'
        AND state IN ('{enums.TokenizerTask.STATE_IN_PROGRESS.value}', '{enums.TokenizerTask.STATE_CREATED.value}')
        AND scope = '{enums.RecordTokenizationScope.ATTRIBUTE.value}'
        AND attribute_name = '{attribute_name}'
        LIMIT 1
    """
    return general.execute_first(query) is not None
