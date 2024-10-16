from typing import List, Optional
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import (
    CognitionStrategyRequirement,
    CognitionStrategyRequirementMappingOption,
)


def get(project_id: str, strategy_requirement_id: str) -> CognitionStrategyRequirement:
    return (
        session.query(CognitionStrategyRequirement)
        .filter(
            CognitionStrategyRequirement.project_id == project_id,
            CognitionStrategyRequirement.id == strategy_requirement_id,
        )
        .first()
    )


def get_all_by_strategy(
    project_id: str,
    strategy_id: str,
    filter_input: bool = False,
    filter_output: bool = False,
) -> List[CognitionStrategyRequirement]:
    query = session.query(CognitionStrategyRequirement).filter(
        CognitionStrategyRequirement.project_id == project_id,
        CognitionStrategyRequirement.strategy_id == strategy_id,
    )

    if filter_input:
        query = query.filter(CognitionStrategyRequirement.is_input == True)
    if filter_output:
        query = query.filter(CognitionStrategyRequirement.is_input == False)

    return query.all()


def get_all_mapping_options_by_strategy(
    project_id: str, strategy_id: str, is_input: bool = True
) -> List[CognitionStrategyRequirementMappingOption]:
    return (
        session.query(
            CognitionStrategyRequirementMappingOption.value,
            CognitionStrategyRequirement.field,
        )
        .join(
            CognitionStrategyRequirement,
            CognitionStrategyRequirement.id
            == CognitionStrategyRequirementMappingOption.strategy_requirement_id,
        )
        .filter(
            CognitionStrategyRequirement.strategy_id == strategy_id,
            CognitionStrategyRequirementMappingOption.project_id == project_id,
            CognitionStrategyRequirement.is_input == is_input,
        )
        .all()
    )


def get_all_mapping_options_of_requirement(
    project_id: str, strategy_requirement_id: str
) -> List[CognitionStrategyRequirementMappingOption]:
    return (
        session.query(CognitionStrategyRequirementMappingOption)
        .filter(
            CognitionStrategyRequirementMappingOption.project_id == project_id,
            CognitionStrategyRequirementMappingOption.strategy_requirement_id
            == strategy_requirement_id,
        )
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    strategy_id: str,
    field: str,
    description: str,
    is_input: bool,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionStrategyRequirement:
    strategy: CognitionStrategyRequirement = CognitionStrategyRequirement(
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        strategy_id=strategy_id,
        field=field,
        description=description,
        is_input=is_input,
    )
    general.add(strategy, with_commit)

    return strategy


def create_mapping_options(
    project_id: str,
    user_id: str,
    strategy_requirement_id: str,
    values: List[str],
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> List[CognitionStrategyRequirementMappingOption]:
    mapping_options: List[CognitionStrategyRequirementMappingOption] = []
    for value in values:
        mapping_option: CognitionStrategyRequirementMappingOption = (
            CognitionStrategyRequirementMappingOption(
                project_id=project_id,
                created_by=user_id,
                created_at=created_at,
                strategy_requirement_id=strategy_requirement_id,
                value=value,
            )
        )
        mapping_options.append(mapping_option)

    general.add_all(mapping_options, with_commit)

    return mapping_options


def update_mapping_options(
    project_id: str,
    user_id: str,
    strategy_requirement_id: str,
    values: List[str],
    with_commit: bool = True,
) -> List[CognitionStrategyRequirementMappingOption]:
    delete_mapping_options(project_id, strategy_requirement_id, False)
    return create_mapping_options(
        project_id, user_id, strategy_requirement_id, values, with_commit
    )


def update(
    project_id: str,
    strategy_requirement_id: str,
    field: Optional[str] = None,
    description: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionStrategyRequirement:
    strategy_requirement: CognitionStrategyRequirement = get(
        project_id, strategy_requirement_id
    )

    if field is not None:
        strategy_requirement.field = field
    if description is not None:
        strategy_requirement.description = description

    general.flush_or_commit(with_commit)
    return strategy_requirement


def delete(
    project_id: str, strategy_requirement_id: str, with_commit: bool = True
) -> None:
    session.query(CognitionStrategyRequirement).filter(
        CognitionStrategyRequirement.project_id == project_id,
        CognitionStrategyRequirement.id == strategy_requirement_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_all_by_strategy(
    project_id: str, strategy_id: str, with_commit: bool = True
) -> None:
    session.query(CognitionStrategyRequirement).filter(
        CognitionStrategyRequirement.project_id == project_id,
        CognitionStrategyRequirement.strategy_id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_mapping_options(
    project_id: str, strategy_requirement_id: str, with_commit: bool = True
) -> None:
    session.query(CognitionStrategyRequirementMappingOption).filter(
        CognitionStrategyRequirementMappingOption.project_id == project_id,
        CognitionStrategyRequirementMappingOption.strategy_requirement_id
        == strategy_requirement_id,
    ).delete()
    general.flush_or_commit(with_commit)
