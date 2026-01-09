from typing import Dict, List, Optional

from submodules.model.business_objects import general
from submodules.model.session import session
from submodules.model import DataBlock, DataBlockResults
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


def get_by_project_id(org_id: str, project_id: str) -> List[DataBlock]:
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
    )
    general.add(data_block, with_commit)
    return data_block


def update(
    org_id: str,
    data_block_id: str,
    name: str,
    description: str,
    with_commit: bool = True,
) -> DataBlock:
    data_block = get(org_id, data_block_id)

    if name:
        data_block.name = name
    if description:
        data_block.description = description
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


def get_result(project_id: str, data_block_id: str) -> Optional[DataBlockResults]:
    return (
        session.query(DataBlockResults)
        .filter(
            DataBlockResults.project_id == project_id,
            DataBlockResults.data_block_id == data_block_id,
        )
        .first()
    )


def get_results_by_data_block_id(
    project_id: str, data_block_id: str
) -> List[DataBlockResults]:
    return (
        session.query(DataBlockResults)
        .filter(
            DataBlockResults.project_id == project_id,
            DataBlockResults.data_block_id == data_block_id,
        )
        .all()
    )


def get_results_by_project_id(project_id: str) -> List[DataBlockResults]:
    return (
        session.query(DataBlockResults)
        .filter(DataBlockResults.project_id == project_id)
        .all()
    )


def create_result(
    project_id: str,
    data_block_id: str,
    data: Dict,
    with_commit: bool = True,
) -> DataBlockResults:
    result = DataBlockResults(
        project_id=project_id,
        data_block_id=data_block_id,
        data=data,
    )
    general.add(result, with_commit)
    return result


def update_result(
    project_id: str,
    data_block_id: str,
    data: Optional[Dict] = None,
    with_commit: bool = True,
) -> Optional[DataBlockResults]:
    result = get_result(project_id, data_block_id)
    if not result:
        return None

    if data is not None:
        result.data = data
    general.add(result, with_commit)
    return result


def delete_result(project_id: str, result_id: str, with_commit: bool = True) -> None:
    session.query(DataBlockResults).filter(
        DataBlockResults.project_id == project_id,
        DataBlockResults.id == result_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_results_by_data_block_id(
    project_id: str, data_block_id: str, with_commit: bool = True
) -> None:
    session.query(DataBlockResults).filter(
        DataBlockResults.project_id == project_id,
        DataBlockResults.data_block_id == data_block_id,
    ).delete()
    general.flush_or_commit(with_commit)
