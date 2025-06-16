from typing import Optional, Type
from datetime import datetime
from sqlalchemy import func

import pytz

from submodules.model.session import session
from submodules.model.models import CognitionIntegration, IntegrationSharepoint


def get_modified_since(integration: CognitionIntegration) -> Optional[datetime]:
    modified = (
        session.query(func.max(IntegrationSharepoint.modified))
        .filter(IntegrationSharepoint.integration_id == integration.id)
        .first()
    )[0] or datetime(1970, 1, 1)
    return pytz.UTC.localize(modified)


from typing import List, Optional, Dict, Union

from sqlalchemy import func
from datetime import datetime

from ..business_objects import general
from ..cognition_objects import integration as integration_db_bo
from ..session import session
from ..enums import IntegrationMetadata


def get(
    IntegrationModel: Type, integration_id: str, id: Optional[str] = None
) -> object:
    query = session.query(IntegrationModel).filter(
        IntegrationModel.integration_id == integration_id,
    )
    if id is not None:
        query = query.filter(IntegrationModel.id == id)
        return query.first()
    return query.order_by(IntegrationModel.created_at.desc()).all()


def get_by_id(IntegrationModel: Type, id: str) -> object:
    return session.query(IntegrationModel).filter(IntegrationModel.id == id).first()


def get_by_running_id(
    IntegrationModel: Type, integration_id: str, running_id: int
) -> object:
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration_id,
            IntegrationModel.running_id == running_id,
        )
        .first()
    )


def get_by_source(IntegrationModel: Type, integration_id: str, source: str) -> object:
    return (
        session.query(IntegrationModel)
        .filter(
            IntegrationModel.integration_id == integration_id,
            IntegrationModel.source == source,
        )
        .first()
    )


def get_all_by_integration_id(
    IntegrationModel: Type, integration_id: str
) -> List[object]:
    return (
        session.query(IntegrationModel)
        .filter(IntegrationModel.integration_id == integration_id)
        .order_by(IntegrationModel.created_at)
        .all()
    )


def get_all_by_project_id(IntegrationModel: Type, project_id: str) -> List[object]:
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
    IntegrationModel, integration_id: str
) -> Dict[str, object]:
    return {
        record.source: record
        for record in get_all_by_integration_id(IntegrationModel, integration_id)
    }


def get_running_ids(IntegrationModel: Type, integration_id: str) -> int:
    return dict(
        session.query(
            IntegrationModel.source,
            func.coalesce(func.max(IntegrationModel.running_id), 0),
        )
        .filter(IntegrationModel.integration_id == integration_id)
        .group_by(IntegrationModel.source)
        .all()
    )


def create(
    IntegrationModel: Type,
    created_by: str,
    integration_id: str,
    running_id: int,
    created_at: Optional[datetime] = None,
    id: Optional[str] = None,
    with_commit: bool = True,
    **metadata,
) -> object:
    integration_record = IntegrationModel(
        created_by=created_by,
        integration_id=integration_id,
        running_id=running_id,
        created_at=created_at,
        id=id,
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
    with_commit: bool = True,
    **metadata,
) -> object:
    integration_record = get(IntegrationModel, integration_id, id)
    integration_record.updated_by = updated_by

    if running_id is not None:
        integration_record.running_id = running_id
    if updated_at is not None:
        integration_record.updated_at = updated_at

    record_updated = False
    for key, value in metadata.items():
        if not hasattr(integration_record, key):
            raise ValueError(
                f"Invalid field '{key}' for {IntegrationModel.__tablename__}"
            )
        existing_value = getattr(integration_record, key, None)
        if value is not None and value != existing_value:
            setattr(integration_record, key, value)
            record_updated = True

    if record_updated:
        general.add(integration_record, with_commit=with_commit)

    return integration_record


def delete_many(
    IntegrationModel: Type,
    ids: List[str],
    with_commit: bool = False,
) -> None:
    integration_records = session.query(IntegrationModel).filter(
        IntegrationModel.id.in_(ids)
    )
    integration_records.delete(synchronize_session=False)
    general.flush_or_commit(with_commit)


def clear_history(IntegrationModel: Type, id: str, with_commit: bool = False) -> None:
    integration_record = get_by_id(IntegrationModel, id)
    integration_record.delta_criteria = None
    general.add(integration_record, with_commit)


def _get_supported_metadata(
    table_name: str, metadata: Dict[str, Union[str, int, float, bool]]
) -> None:
    supported_keys = IntegrationMetadata.from_table_name(table_name)
    supported_metadata = {
        key: metadata[key] for key in supported_keys.intersection(metadata.keys())
    }
    return __rename_metadata(table_name, supported_metadata)


def __rename_metadata(
    table_name: str, metadata: Dict[str, Union[str, int, float, bool]]
) -> Dict[str, object]:
    rename_keys = {
        "id": f"{table_name}_id",
        "created_by": f"{table_name}_created_by",
        "created_at": f"{table_name}_created_at",
        "updated_by": f"{table_name}_updated_by",
        "updated_at": f"{table_name}_updated_at",
    }
    return {rename_keys.get(key, key): value for key, value in metadata.items()}
