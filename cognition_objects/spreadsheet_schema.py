from typing import List, Optional, Dict
from ..business_objects import general
from ..session import session
from ..models import CognitionSpreadsheetSchema
from datetime import datetime


def get(spreadsheet_schema_id: str) -> CognitionSpreadsheetSchema:
    return session.query(CognitionSpreadsheetSchema).filter(
        CognitionSpreadsheetSchema.id == spreadsheet_schema_id,
    ).first()

def get_schema_elements_by_project(project_id: str) -> List[CognitionSpreadsheetSchema]:
    return (
        session.query(CognitionSpreadsheetSchema)
        .filter(CognitionSpreadsheetSchema.cognition_project_id == project_id)
        .all()
    )


def create(
    name: str,
    description: str,
    project_id: str,
    user_id: str,
    data_type: str,
    is_input: bool,
    is_hidden: bool,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionSpreadsheetSchema:    
    schema: CognitionSpreadsheetSchema = CognitionSpreadsheetSchema(
        name=name,
        description=description,
        created_by=user_id,
        created_at=created_at,
        data_type=data_type,
        is_input=is_input,
        is_hidden=is_hidden,
        cognition_project_id=project_id,
    )
    general.add(schema, with_commit)
    return schema


def update(
    spreadsheet_schema_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    data_type: Optional[str] = None,
    is_input: Optional[bool] = None,
    is_hidden: Optional[bool] = None,
    with_commit: bool = True,
) -> CognitionSpreadsheetSchema:
    schema: CognitionSpreadsheetSchema = get(spreadsheet_schema_id)
    if name is not None:
        schema.name = name

    if description is not None:
        schema.description = description
    if data_type is not None:
        schema.data_type = data_type
    if is_input is not None:
        schema.is_input = is_input
    if is_hidden is not None:
        schema.is_hidden = is_hidden
    general.flush_or_commit(with_commit)
    return schema


def delete(spreadsheet_schema_id: str, with_commit: bool = True) -> None:
    session.query(CognitionSpreadsheetSchema).filter(
        CognitionSpreadsheetSchema.id == spreadsheet_schema_id,
    ).delete()
    general.flush_or_commit(with_commit)
