from typing import Dict, List, Optional, Tuple, Any

from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionSynopsisSpreadsheetRow


def get(spreadsheet_row_id: str) -> CognitionSynopsisSpreadsheetRow:
    return (
        session.query(CognitionSynopsisSpreadsheetRow)
        .filter(
            CognitionSynopsisSpreadsheetRow.id == spreadsheet_row_id,
        )
        .first()
    )


def get_all_by_spreadsheet_id(
    spreadsheet_id: str,
) -> List[CognitionSynopsisSpreadsheetRow]:
    return (
        session.query(CognitionSynopsisSpreadsheetRow)
        .filter(
            CognitionSynopsisSpreadsheetRow.spreadsheet_id == spreadsheet_id,
        )
        .order_by(CognitionSynopsisSpreadsheetRow.created_at.asc())
        .all()
    )


def create(
    spreadsheet_id: str,
    project_id: str,
    user_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionSynopsisSpreadsheetRow:
    spreadsheet_row = CognitionSynopsisSpreadsheetRow(
        project_id=project_id,
        spreadsheet_id=spreadsheet_id,
        created_at=created_at,
        created_by=user_id,
    )
    general.add(spreadsheet_row, with_commit)
    return spreadsheet_row


def delete(
    project_id: str,
    spreadsheet_id: str,
    spreadsheet_row_id: str,
    with_commit: bool = True,
) -> None:
    session.query(CognitionSynopsisSpreadsheetRow).filter(
        CognitionSynopsisSpreadsheetRow.id == spreadsheet_row_id,
        CognitionSynopsisSpreadsheetRow.spreadsheet_id == spreadsheet_id,
        CognitionSynopsisSpreadsheetRow.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)
