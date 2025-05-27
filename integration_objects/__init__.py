from typing import List, Optional

from datetime import datetime

from ..business_objects import general
from ..cognition_objects import integration as integration_db_bo
from ..session import session
from ..enums import IntegrationMetadata


def get_by_id(IntegrationModel, id: str) -> object:
    return session.query(IntegrationModel).filter(IntegrationModel.id == id).first()


def get_by_running_id(IntegrationModel, integration_id: str, running_id: int) -> object:
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration_id,
            IntegrationModel.running_id == running_id,
        )
        .first()
    )


def get_by_source(IntegrationModel, integration_id: str, source: str) -> object:
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration_id,
            IntegrationModel.source == source,
        )
        .first()
    )


def get_all_by_integration_id(IntegrationModel, integration_id: str) -> List[object]:
    return (
        session.query(IntegrationModel)
        .filter(IntegrationModel.integration_id == integration_id)
        .order_by(IntegrationModel.created_at)
        .all()
    )


def get_all_by_project_id(IntegrationModel, project_id: str) -> List[object]:
    integrations = integration_db_bo.get_all_by_project_id(project_id)
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id.in_([i.id for i in integrations]),
        )
        .order_by(IntegrationModel.created_at.asc())
        .all()
    )


def create(
    IntegrationModel,
    created_by: str,
    integration_id: str,
    running_id: int,
    created_at: Optional[datetime] = None,
    id: Optional[str] = None,
    with_commit: bool = True,
    **metadata,
) -> object:
    kwargs = __get_supported_metadata(IntegrationModel.__tablename__, **metadata)
    integration_record = IntegrationModel(
        created_by=created_by,
        integration_id=integration_id,
        running_id=running_id,
        created_at=created_at,
        id=id,
        **kwargs,
    )

    general.add(integration_record, with_commit)

    return integration_record


def update(
    IntegrationModel,
    id: str,
    updated_by: str,
    running_id: Optional[int] = None,
    updated_at: Optional[datetime] = None,
    **metadata,
) -> object:
    integration_record = get_by_id(IntegrationModel, id)
    integration_record.updated_by = updated_by

    if running_id is not None:
        integration_record.running_id = running_id
    if updated_at is not None:
        integration_record.updated_at = updated_at

    record_updated = False
    kwargs = __get_supported_metadata(IntegrationModel.__tablename__, **metadata)
    for key, value in kwargs.items():
        if not hasattr(integration_record, key):
            raise ValueError(
                f"Invalid field '{key}' for {IntegrationModel.__tablename__}"
            )
        existing_value = getattr(integration_record, key, None)
        if value is not None and value != existing_value:
            setattr(integration_record, key, value)
            record_updated = True

    general.add(integration_record, with_commit=record_updated)

    return integration_record


def delete_many(IntegrationModel, ids: List[str], with_commit: bool = False) -> None:
    integration_records = session.query(IntegrationModel).filter(
        IntegrationModel.id.in_(ids)
    )
    integration_records.delete(synchronize_session=False)
    general.flush_or_commit(with_commit)


def clear_history(IntegrationModel, id: str, with_commit: bool = False) -> None:
    integration_record = get_by_id(IntegrationModel, id)
    integration_record.delta_criteria = None
    general.add(integration_record, with_commit)


def __get_supported_metadata(table_name: str, **kwargs) -> None:
    supported_keys = IntegrationMetadata.from_table_name(table_name)
    return {key: kwargs[key] for key in supported_keys.intersection(kwargs.keys())}


__all__ = [
    "create",
    "update",
    "delete_many",
    "clear_history",
    "get_by_id",
    "get_by_running_id",
    "get_all_by_integration_id",
    "get_all_by_project_id",
]
