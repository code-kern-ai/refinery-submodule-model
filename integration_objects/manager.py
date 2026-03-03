import re
from typing import List, Optional, Dict, Tuple, Union, Type, Any
from datetime import datetime
from sqlalchemy import func, or_, and_
from sqlalchemy.orm.attributes import flag_modified
from submodules.s3 import enums

from ..enums import CognitionIntegrationType, IntegrationRecordScope
from ..business_objects import general
from ..cognition_objects import integration as integration_db_bo
from ..global_objects import etl_task as etl_task_db_bo
from ..session import session
from .helper import get_integration_record_identifier, get_supported_metadata_keys
from ..models import (
    IntegrationSharepoint,
    IntegrationPdf,
    IntegrationGithubIssue,
    IntegrationGithubFile,
    IntegrationWebpage,
    CognitionIntegration,
    EtlTask,
)
from submodules.model import enums


def get(
    IntegrationModel: Type,
    integration_id: str,
    id: Optional[str] = None,
) -> Union[List[object], object]:
    query = session.query(IntegrationModel).filter(
        IntegrationModel.integration_id == integration_id,
    )
    if id is not None:
        query = query.filter(IntegrationModel.id == id)
        return query.first()
    return query.order_by(IntegrationModel.created_at.desc()).all()


def count(
    integration: CognitionIntegration,
    by: str = "source",
) -> int:
    IntegrationModel = integration_model(integration=integration)
    record_identifier = getattr(IntegrationModel, by, IntegrationModel.source)
    return len(
        (
            session.query(IntegrationModel)
            .filter(
                IntegrationModel.integration_id == integration.id,
                record_identifier.op("regexp")(r"#\d$"),
            )
            .all()
        )
    )


def get_last_record(integration: CognitionIntegration) -> Optional[object]:
    IntegrationModel = integration_model(integration=integration)
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration.id,
        )
        .order_by(IntegrationModel.created_at.desc())
        .first()
    )


def get_record_scope(integration_id: str) -> IntegrationRecordScope:
    integration = integration_db_bo.get_by_id(integration_id)
    last_record = get_last_record(integration)
    if last_record is None:
        return
    etl_task = etl_task_db_bo.get_by_id(last_record.etl_task_id)
    if etl_task is None:
        return

    full_config = etl_task.full_config or []
    splitting_task = next(
        (
            task
            for task in full_config
            if task.get("task_type") == enums.CognitionMarkdownFileState.SPLITTING.value
        ),
        None,
    )
    if (
        splitting_task
        and splitting_task.get("task_config", {}).get("strategy")
        == enums.ETLSplitStrategy.CHUNK.value
    ):
        return IntegrationRecordScope.CHUNKS.value
    return IntegrationRecordScope.ROOT.value


def get_by_id(
    IntegrationModel: Type,
    id: str,
) -> object:
    return session.query(IntegrationModel).filter(IntegrationModel.id == id).first()


def get_by_ids(
    IntegrationModel: Type,
    ids: List[str],
) -> list:
    return session.query(IntegrationModel).filter(IntegrationModel.id.in_(ids)).all()


def get_all_by_etl_task(
    etl_task: EtlTask,
) -> List[object]:
    integration_id: str = etl_task.meta_data.get("integration_id")
    if not integration_id:
        print(
            f"WARNING:  integration_id not found in etl_task meta_data for etl_task_id '{etl_task.id}'",
            flush=True,
        )
        return []
    IntegrationModel = integration_model(integration_id=integration_id)
    return (
        session.query(IntegrationModel)
        .filter(IntegrationModel.etl_task_id == etl_task.id)
        .all()
    )


def get_by_running_id(
    IntegrationModel: Type,
    integration_id: str,
    running_id: int,
) -> object:
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration_id,
            IntegrationModel.running_id == running_id,
        )
        .first()
    )


def get_by_source(
    IntegrationModel: Type,
    integration_id: str,
    source: str,
) -> object:
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration_id,
            IntegrationModel.source == source,
        )
        .first()
    )


def get_all_by_integration_id(
    integration_id: str,
    only_refinery_delta: bool = False,
    scope: Optional[IntegrationRecordScope] = None,
) -> Tuple[List[object], Type]:
    IntegrationModel = integration_model(integration_id)
    query = session.query(IntegrationModel).filter(
        IntegrationModel.integration_id == integration_id
    )

    if only_refinery_delta:
        integration = integration_db_bo.get_by_id(integration_id)

        if integration and integration.delta_criteria:
            delta_record_ids = integration.delta_criteria.get("delta_record_ids", [])

            if delta_record_ids:
                if scope == IntegrationRecordScope.ALL.value:
                    delta_record_ids = [
                        record.id
                        for record in get_related_chunk_records_by_ids(
                            integration_id, delta_record_ids
                        )
                    ] + delta_record_ids
                if scope == IntegrationRecordScope.CHUNKS.value:
                    by = get_integration_record_identifier(integration)
                    delta_record_ids = [
                        record.id
                        for record in get_related_chunk_records_by_ids(
                            integration_id, delta_record_ids, by=by
                        )
                    ]
                # TODO check uuid or str cast
                query = query.filter(IntegrationModel.id.in_(delta_record_ids))

    if scope:
        integration_entity = integration_db_bo.get_by_id(integration_id)
        record_identifier = getattr(
            IntegrationModel,
            get_integration_record_identifier(integration=integration_entity),
            IntegrationModel.source,
        )
        query = query.filter(
            record_identifier.like(
                "%#%" if scope == IntegrationRecordScope.CHUNKS.value else "%[^#]%"
            )
        )
    return (
        query.order_by(IntegrationModel.created_at).all(),
        IntegrationModel,
    )


def integration_model(
    integration_id: Optional[str] = None,
    integration: Optional[CognitionIntegration] = None,
) -> Type:
    if not integration_id and not integration:
        raise ValueError("Either integration_id or integration must be provided")
    integration = integration or integration_db_bo.get_by_id(integration_id)
    if integration.type == CognitionIntegrationType.SHAREPOINT.value:
        return IntegrationSharepoint
    elif integration.type == CognitionIntegrationType.PDF.value:
        return IntegrationPdf
    elif integration.type == CognitionIntegrationType.GITHUB_FILE.value:
        return IntegrationGithubFile
    elif integration.type == CognitionIntegrationType.GITHUB_ISSUE.value:
        return IntegrationGithubIssue
    elif integration.type == CognitionIntegrationType.WEBPAGE.value:
        return IntegrationWebpage
    else:
        raise ValueError(f"Unsupported integration type: {integration.type}")


def get_all_by_project_id(
    IntegrationModel: Type,
    project_id: str,
) -> List[object]:
    integrations = integration_db_bo.get_all_by_project_id(project_id)
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id.in_([i.id for i in integrations]),
        )
        .order_by(IntegrationModel.created_at.asc())
        .all()
    )


def get_existing_integration_records(
    integration_id: str,
    by: str = "source",
    scope: IntegrationRecordScope = IntegrationRecordScope.ALL.value,
) -> Dict[str, object]:

    records, _ = get_all_by_integration_id(integration_id, scope)

    if scope == IntegrationRecordScope.ROOT.value:
        records = filter(
            lambda x: not re.search(r"#\d$", getattr(x, by, x.source) or ""), records
        )
    elif scope == IntegrationRecordScope.CHUNKS.value:
        records = filter(
            lambda x: re.search(r"#\d$", getattr(x, by, x.source) or ""), records
        )
    records_by = {getattr(record, by, record.source): record for record in records}
    return records_by


def get_related_chunk_records(
    integration_record: object,
    by: str = "source",
) -> List[object]:
    IntegrationModel = type(integration_record)
    record_identifier = getattr(IntegrationModel, by, IntegrationModel.source)
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration_record.integration_id,
        )
        .filter(
            record_identifier.like(
                f"{getattr(integration_record, by, integration_record.source)}#%"
            )
        )
        .all()
    )


def get_related_chunk_records_by_ids(
    integration_id: str,
    integration_record_ids: List[str],
    by: str = "source",
) -> List[object]:
    if not integration_record_ids:
        return []
    IntegrationModel = integration_model(integration_id)
    record_identifier = getattr(integration_model, by, IntegrationModel.source)
    integration_records = get_by_ids(IntegrationModel, integration_record_ids)
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration_id,
        )
        .filter(
            or_(
                *[
                    record_identifier.like(
                        f"{getattr(integration_record, by, integration_record.source)}#%"
                    )
                    for integration_record in integration_records
                ]
            )
        )
        .all()
    )


def get_running_ids(
    integration_id: str,
    by: str = "source",
) -> Dict[str, int]:
    IntegrationModel = integration_model(integration_id)
    return dict(
        session.query(
            getattr(IntegrationModel, by, IntegrationModel.source),
            func.max(IntegrationModel.running_id),
        )
        .filter(IntegrationModel.integration_id == integration_id)
        .group_by(getattr(IntegrationModel, by, IntegrationModel.source))
        .all()
    )


def duplicate(
    integration_record: object,
    content: str,
    running_id: str,
    chunk_idx: int,
    by: str = "source",
) -> object:
    IntegrationModel = type(integration_record)

    duplicated_record = IntegrationModel(
        created_by=integration_record.created_by,
        integration_id=integration_record.integration_id,
        etl_task_id=integration_record.etl_task_id,
        error_message=integration_record.error_message,
        content=content,
        updated_by=integration_record.updated_by,
        updated_at=integration_record.updated_at,
    )

    for key in get_supported_metadata_keys(IntegrationModel.__tablename__):
        value = getattr(integration_record, key)
        setattr(duplicated_record, key, value)

    duplicated_record.running_id = running_id

    new_attr_value = f"{getattr(integration_record, by)}#{chunk_idx}"
    setattr(duplicated_record, by, new_attr_value)

    try:
        general.add(duplicated_record, with_commit=False)
    except Exception as e:
        print("ERROR:    ", str(e), flush=True)
        raise e

    return duplicated_record


def create(
    IntegrationModel: Type,
    created_by: str,
    integration_id: str,
    created_at: Optional[datetime] = None,
    error_message: Optional[str] = None,
    id: Optional[str] = None,
    content: Optional[str] = None,
    with_commit: bool = True,
    **metadata,
) -> Optional[object]:
    if not integration_db_bo.get_by_id(integration_id):
        # If the integration does not exist,
        # it was likely deleted during runtime
        print(
            f"ERROR:     integration with id '{integration_id}' not found", flush=True
        )
        return
    integration_record = IntegrationModel(
        created_by=created_by,
        integration_id=integration_id,
        created_at=created_at,
        error_message=error_message,
        id=id,
        content=content,
        **metadata,
    )

    general.add(integration_record, with_commit)

    return integration_record


def update(
    IntegrationModel: Type,
    id: str,
    integration_id: str,
    updated_by: str,
    running_id: Optional[int] = None,
    updated_at: Optional[datetime] = None,
    error_message: Optional[str] = None,
    etl_task_id: Optional[str] = None,
    content: Optional[str] = None,
    with_commit: bool = True,
    **metadata,
) -> Optional[object]:
    if not integration_db_bo.get_by_id(integration_id):
        # If the integration does not exist,
        # it was likely deleted during runtime
        print(
            f"ERROR:     integration with id '{integration_id}' not found", flush=True
        )
        return

    record_updated = False
    integration_record = get(IntegrationModel, integration_id, id)
    integration_record.updated_by = updated_by

    if running_id is not None:
        integration_record.running_id = running_id
        record_updated = True
    if updated_at is not None:
        integration_record.updated_at = updated_at
        record_updated = True
    if error_message is not None:
        integration_record.error_message = error_message
        record_updated = True
    if content is not None:
        integration_record.content = content
        record_updated = True
    if etl_task_id is not None:
        integration_record.etl_task_id = etl_task_id
        record_updated = True
    for key, value in metadata.items():
        if not hasattr(integration_record, key):
            raise ValueError(
                f"Invalid field '{key}' for {IntegrationModel.__tablename__}"
            )
        existing_value = getattr(integration_record, key, None)
        if value is not None and value != existing_value:
            setattr(integration_record, key, value)
            flag_modified(integration_record, key)
            record_updated = True

    if record_updated:
        general.flush_or_commit(with_commit)

    return integration_record


def delete_many(
    IntegrationModel: Type,
    ids: List[str],
    with_commit: bool = False,
) -> None:
    integration_records = session.query(IntegrationModel).filter(
        IntegrationModel.id.in_(ids)
    )
    etl_task_db_bo.delete_many(
        ids=[record.etl_task_id for record in integration_records]
    )
    integration_records.delete(synchronize_session=False)
    general.flush_or_commit(with_commit)


def clear_history(
    IntegrationModel: Type,
    id: str,
    with_commit: bool = False,
) -> None:
    integration_record = get_by_id(IntegrationModel, id)
    integration_record.delta_criteria = None
    flag_modified(integration_record, "delta_criteria")
    general.flush_or_commit(with_commit)


def get_supported_metadata(
    table_name: str, metadata: Dict[str, Union[str, int, float, bool]]
) -> Dict[str, Any]:
    metadata = __rename_metadata(table_name, metadata)
    supported_keys = get_supported_metadata_keys(table_name)
    supported_metadata = {
        key: metadata[key] for key in supported_keys.intersection(metadata.keys())
    }
    return supported_metadata


def __rename_metadata(
    table_name: str, metadata: Dict[str, Union[str, int, float, bool]]
) -> Dict[str, Any]:
    rename_keys = {
        "id": f"{table_name}_id",
        "created_by": f"{table_name}_created_by",
        "created_at": f"{table_name}_created_at",
        "updated_by": f"{table_name}_updated_by",
        "updated_at": f"{table_name}_updated_at",
    }
    return {rename_keys.get(key, key): value for key, value in metadata.items()}


def get_metadata_from_record(record: object) -> Dict[str, Any]:
    supported_keys = get_supported_metadata_keys(record.__tablename__)
    supported_metadata = {key: getattr(record, key) for key in supported_keys}
    return supported_metadata


def set_refinery_synced_by_record_ids(
    integration_id: str,
    record_ids: List[str],
    with_commit: bool = True,
) -> None:
    IntegrationModel = integration_model(integration_id=integration_id)
    session.query(IntegrationModel).filter(IntegrationModel.id.in_(record_ids)).update(
        {IntegrationModel.refinery_synced: True}, synchronize_session=False
    )
    general.flush_or_commit(with_commit)
