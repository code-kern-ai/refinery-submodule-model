from typing import Any, Dict, List
from ..session import session
from submodules.model.business_objects import general
from submodules.model.models import DataManagerOrganizationQuestions, Team


def create(
    org_id: str,
    type: str,
    config: Dict[str, Any],
    with_commit: bool = True,
) -> DataManagerOrganizationQuestions:
    new_question = DataManagerOrganizationQuestions(
        organization_id=org_id,
        type=type,
        config=config,
    )

    general.add(new_question, with_commit)
    return new_question


def get_all(org_id: str) -> List[DataManagerOrganizationQuestions]:
    return (
        session.query(DataManagerOrganizationQuestions)
        .filter(DataManagerOrganizationQuestions.organization_id == org_id)
        .all()
    )
