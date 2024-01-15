from typing import Dict, List, Optional, Tuple, Any

from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionSynopsisSpreadsheet


def get(project_id: str, spreadsheet_id: str) -> CognitionSynopsisSpreadsheet:
    return (
        session.query(CognitionSynopsisSpreadsheet)
        .filter(
            CognitionSynopsisSpreadsheet.id == spreadsheet_id,
            CognitionSynopsisSpreadsheet.project_id == project_id,
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
    name: str,
    synopsis_type: str,
    dataset_id: Optional[str] = None,
    filter_name: Optional[str] = None,
    task_scope_dict: Optional[Dict[str, Any]] = None,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionSynopsisSpreadsheet:
    spreadsheet = CognitionSynopsisSpreadsheet(
        project_id=project_id,
        dataset_id=dataset_id,
        name=name,
        filter_attribute_name=filter_name,
        synopsis_type=synopsis_type,
        task_scope_dict=task_scope_dict,
        created_at=created_at,
        created_by=user_id,
    )
    general.add(spreadsheet, with_commit)
    return spreadsheet


def update(
    project_id: str,
    spreadsheet_id: str,
    name: Optional[str] = None,
    filter_name: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionSynopsisSpreadsheet:
    spreadsheet = get(project_id, spreadsheet_id)
    if name is not None:
        spreadsheet.name = name
    if filter_name is not None:
        spreadsheet.filter_name = filter_name

    general.flush_or_commit(with_commit)
    return spreadsheet


def delete(project_id: str, spreadsheet_id: str, with_commit: bool = True) -> None:
    session.query(CognitionSynopsisSpreadsheet).filter(
        CognitionSynopsisSpreadsheet.id == spreadsheet_id,
        CognitionSynopsisSpreadsheet.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)
