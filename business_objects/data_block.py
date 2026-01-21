from typing import Dict, List, Optional, Any

from submodules.model.business_objects import general
from submodules.model.session import session
from submodules.model import DataBlock
from submodules.model.enums import DataBlockType


def get(org_id: str, id: str) -> DataBlock:
    return (
        session.query(DataBlock)
        .filter(
            DataBlock.organization_id == org_id,
            DataBlock.id == id,
        )
        .first()
    )


def get_by_id(data_block_id: str) -> DataBlock:
    """Get a data block by ID without requiring org_id."""
    return session.query(DataBlock).filter(DataBlock.id == data_block_id).first()


def get_all_by_project_id(org_id: str, project_id: str) -> List[DataBlock]:
    return (
        session.query(DataBlock)
        .filter(
            DataBlock.organization_id == org_id,
            DataBlock.project_id == project_id,
        )
        .all()
    )


def get_by_project_id_and_type(
    org_id: str, project_id: str, type: DataBlockType
) -> DataBlock:
    return (
        session.query(DataBlock)
        .filter(
            DataBlock.organization_id == org_id,
            DataBlock.project_id == project_id,
            DataBlock.type == type.value,
        )
        .first()
    )


def create(
    org_id: str,
    user_id: str,
    project_id: str,
    name: str,
    description: str,
    type: DataBlockType,
    with_commit: bool = True,
) -> DataBlock:
    data_block = DataBlock(
        organization_id=org_id,
        created_by=user_id,
        project_id=project_id,
        name=name,
        description=description,
        type=type.value,
        sql_data=[],
    )
    general.add(data_block, with_commit)
    return data_block


def update(
    org_id: str,
    data_block_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    sql_config: Optional[Dict[str, Dict[str, Any]]] = None,
    sql_data: Optional[List[Dict[str, str]]] = None,
    overwrite_sql: bool = False,
    with_commit: bool = True,
) -> DataBlock:
    data_block = get(org_id, data_block_id)

    if name:
        data_block.name = name
    if description:
        data_block.description = description

    if overwrite_sql:
        if sql_data is not None:
            data_block.sql_data = sql_data
        if sql_config is not None:
            data_block.sql_config = sql_config
    else:
        if sql_data is not None:
            if not data_block.sql_data:
                data_block.sql_data = []
            data_block.sql_data.extend(sql_data)
        if sql_config is not None:
            if not data_block.sql_config:
                data_block.sql_config = {}
            data_block.sql_config.update(sql_config)

    general.add(data_block, with_commit)
    return data_block


def delete_many(
    org_id: str, project_id: str, ids: List[str], with_commit: bool = False
) -> None:
    session.query(DataBlock).filter(
        DataBlock.organization_id == org_id,
        DataBlock.project_id == project_id,
        DataBlock.id.in_(ids),
    ).delete()
    general.flush_or_commit(with_commit)
