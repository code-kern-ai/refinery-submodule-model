from datetime import datetime
from typing import Dict, Any, List, Optional

from sqlalchemy import func
from sqlalchemy.orm.attributes import flag_modified

from . import general
from ..enums import Tablenames, AttributeState, DataTypes
from ..models import DataBlockAttribute
from ..business_objects.attribute import DEFAULT_ATTRIBUTE_STATES_USEABLE
from ..session import session
from ..util import prevent_sql_injection


def get(data_block_id: str, attribute_id: str) -> DataBlockAttribute:
    return (
        session.query(DataBlockAttribute)
        .filter(
            DataBlockAttribute.data_block_id == data_block_id,
            DataBlockAttribute.id == attribute_id,
        )
        .first()
    )


def get_by_name(data_block_id: str, name: str) -> DataBlockAttribute:
    return (
        session.query(DataBlockAttribute)
        .filter(
            DataBlockAttribute.data_block_id == data_block_id,
            DataBlockAttribute.name == name,
        )
        .first()
    )


def get_all(
    data_block_id: str,
    state_filter: Optional[List[str]] = None,
    user_created: Optional[bool] = None,
) -> List[DataBlockAttribute]:
    query = session.query(DataBlockAttribute).filter(
        DataBlockAttribute.data_block_id == data_block_id
    )
    if state_filter is not None:
        query = query.filter(DataBlockAttribute.state.in_(state_filter))
    if user_created is not None:
        query = query.filter(DataBlockAttribute.user_created == user_created)
    return query.order_by(DataBlockAttribute.relative_position.asc()).all()


def get_all_by_ids(
    data_block_id: str,
    attribute_ids: List[str],
) -> List[DataBlockAttribute]:
    return (
        session.query(DataBlockAttribute)
        .filter(
            DataBlockAttribute.data_block_id == data_block_id,
            DataBlockAttribute.id.in_(attribute_ids),
        )
        .all()
    )


def get_all_by_names(
    data_block_id: str, attribute_names: List[str]
) -> List[DataBlockAttribute]:
    return (
        session.query(DataBlockAttribute)
        .filter(
            DataBlockAttribute.data_block_id == data_block_id,
            DataBlockAttribute.name.in_(attribute_names),
        )
        .all()
    )


def get_max_relative_position(data_block_id: str) -> int:
    result = (
        session.query(func.max(DataBlockAttribute.relative_position))
        .filter(DataBlockAttribute.data_block_id == data_block_id)
        .first()
    )
    return result[0] if result and result[0] is not None else 0


def create(
    data_block_id: str,
    name: str,
    relative_position: int,
    data_type: str = DataTypes.TEXT.value,
    user_created: bool = False,
    source_code: Optional[str] = None,
    state: Optional[str] = None,
    logs: Optional[List[str]] = None,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    progress: Optional[float] = None,
    additional_config: Optional[Dict[str, Any]] = None,
    with_commit: bool = False,
) -> DataBlockAttribute:
    attribute = DataBlockAttribute(
        data_block_id=data_block_id,
        name=name,
        data_type=data_type,
        relative_position=relative_position,
        user_created=user_created,
    )

    if source_code is not None:
        attribute.source_code = source_code

    if state is not None:
        attribute.state = state

    if logs is not None:
        attribute.logs = logs

    if started_at is not None:
        attribute.started_at = started_at

    if finished_at is not None:
        attribute.finished_at = finished_at

    if progress is not None:
        attribute.progress = progress

    if additional_config is not None:
        attribute.additional_config = additional_config

    general.add(attribute, with_commit)
    return attribute


def create_many(
    data_block_id: str,
    attributes: List[Dict[str, Any]],
    with_commit: bool = False,
) -> List[DataBlockAttribute]:
    """
    Create multiple attributes at once.
    Each attribute dict should contain: name, data_type, and optionally state.
    """
    relative_position = 0  # get_relative_position(data_block_id)
    created_attributes = []

    for idx, attr in enumerate(attributes):
        relative_position += idx + 1
        attribute = create(
            data_block_id=data_block_id,
            name=attr.get("column_name"),
            data_type=attr.get("column_data_type", DataTypes.TEXT.value),
            relative_position=relative_position,
            user_created=attr.get("user_created", False),
            state=attr.get("state", AttributeState.AUTOMATICALLY_CREATED.value),
            is_primary_key=attr.get("is_primary_key", False),
            additional_config=attr.get("additional_config", {}),
            with_commit=False,
        )
        created_attributes.append(attribute)

    for attr in get_all(data_block_id, user_created=True):
        relative_position += 1
        update(
            data_block_id=data_block_id,
            attribute_id=attr.id,
            relative_position=relative_position,
            with_commit=False,
        )

    general.flush_or_commit(with_commit)
    return created_attributes


def update(
    data_block_id: str,
    attribute_id: str,
    name: Optional[str] = None,
    data_type: Optional[str] = None,
    relative_position: Optional[int] = None,
    user_created: Optional[bool] = None,
    source_code: Optional[str] = None,
    state: Optional[str] = None,
    logs: Optional[List[str]] = None,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    progress: Optional[float] = None,
    additional_config: Optional[Dict[str, Any]] = None,
    with_commit: bool = False,
) -> DataBlockAttribute:
    attribute = get(data_block_id, attribute_id)
    if not attribute:
        return None

    if name is not None:
        attribute.name = name
    if data_type is not None:
        attribute.data_type = data_type
    if relative_position is not None:
        attribute.relative_position = relative_position
    if user_created is not None:
        attribute.user_created = user_created
    if source_code is not None:
        attribute.source_code = source_code
    if state is not None:
        attribute.state = state
    if logs is not None:
        attribute.logs = logs
        flag_modified(attribute, "logs")
    if started_at is not None:
        attribute.started_at = started_at
    if finished_at is not None:
        attribute.finished_at = finished_at
    if progress is not None:
        attribute.progress = progress
    if additional_config is not None:
        attribute.additional_config = additional_config
        flag_modified(attribute, "additional_config")

    general.flush_or_commit(with_commit)
    return attribute


def delete(data_block_id: str, attribute_id: str, with_commit: bool = False) -> None:
    session.query(DataBlockAttribute).filter(
        DataBlockAttribute.data_block_id == data_block_id,
        DataBlockAttribute.id == attribute_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_automatically_created(data_block_id: str, with_commit: bool = False) -> None:
    session.query(DataBlockAttribute).filter(
        DataBlockAttribute.data_block_id == data_block_id,
        DataBlockAttribute.state == AttributeState.AUTOMATICALLY_CREATED.value,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_many(
    data_block_id: str, attribute_ids: List[str], with_commit: bool = False
) -> None:
    session.query(DataBlockAttribute).filter(
        DataBlockAttribute.data_block_id == data_block_id,
        DataBlockAttribute.id.in_(attribute_ids),
    ).delete()
    general.flush_or_commit(with_commit)


def delete_user_created_attribute(
    data_block_id: str, attribute_name: str, with_commit: bool = False
) -> None:
    data_block_id = prevent_sql_injection(data_block_id, isinstance(data_block_id, str))
    attribute_name = prevent_sql_injection(
        attribute_name, isinstance(attribute_name, str)
    )
    sql = f"""
    UPDATE {Tablenames.DATA_BLOCK.value}
    SET sql_data = ARRAY(
        SELECT (elem::jsonb - '{attribute_name}')::json
        FROM unnest(sql_data) AS elem
    )
    WHERE id = '{data_block_id}'
    """
    general.execute(sql)
    general.flush_or_commit(with_commit)


def sync_attributes_from_schema(
    data_block_id: str,
    schema: List[Dict[str, str]],
    with_commit: bool = False,
) -> List[DataBlockAttribute]:
    """
    Synchronize attributes from a schema definition.
    This replaces the old sql_schema column functionality.

    Args:
        data_block_id: The ID of the data block
        schema: List of dicts with column_name, column_data_type, and optionally state
        with_commit: Whether to commit the transaction

    Returns:
        List of created/updated DataBlockAttribute
    """
    # Delete existing attributes for this data block
    delete_automatically_created(data_block_id, with_commit=False)

    # Create new attributes from schema
    return create_many(data_block_id, schema, with_commit=with_commit)


def get_schema_as_list(data_block_id: str) -> List[Dict[str, str]]:
    """
    Get attributes as a list of dicts (mimicking the old sql_schema format).
    This provides backward compatibility for code expecting the old format.
    """
    attributes = get_all(data_block_id, state_filter=None)
    return [
        {
            "id": attr.id,
            "column_name": attr.name,
            "column_data_type": attr.data_type,
            "state": attr.state,
            "user_created": attr.user_created,
            "additional_config": (
                attr.additional_config if attr.additional_config else {}
            ),
        }
        for attr in attributes
    ]
