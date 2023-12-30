from typing import Dict, List, Optional, Tuple, Any

from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionSynopsisSpreadsheet


def get(spreadsheet_id: str) -> CognitionSynopsisSpreadsheet:
    return (
        session.query(CognitionSynopsisSpreadsheet)
        .filter(
            CognitionSynopsisSpreadsheet.id == spreadsheet_id,
        )
        .first()
    )


def get_all_by_project_id(
    project_id: str,
) -> List[CognitionSynopsisSpreadsheet]:
    return (
        session.query(CognitionSynopsisSpreadsheet)
        .filter(
            CognitionSynopsisSpreadsheet.project_id == project_id,
        )
        .order_by(CognitionSynopsisSpreadsheet.created_at.asc())
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    dataset_id: str,
    name: str,
    filter_name: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionSynopsisSpreadsheet:
    spreadsheet = CognitionSynopsisSpreadsheet(
        project_id=project_id,
        dataset_id=dataset_id,
        name=name,
        filter_attribute_name=filter_name,
        created_at=created_at,
        created_by=user_id,
    )
    general.add(spreadsheet, with_commit)
    return spreadsheet


def update(
    spreadsheet_id: str,
    name: Optional[str] = None,
    filter_name: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionSynopsisSpreadsheet:
    spreadsheet = get(spreadsheet_id)
    if name is not None:
        spreadsheet.name = name
    if filter_name is not None:
        spreadsheet.filter_name = filter_name

    general.flush_or_commit(with_commit)
    return spreadsheet


def delete(spreadsheet_id: str, with_commit: bool = True) -> None:
    session.query(CognitionSynopsisSpreadsheet).filter(
        CognitionSynopsisSpreadsheet.id == spreadsheet_id,
    ).delete()
    general.flush_or_commit(with_commit)
