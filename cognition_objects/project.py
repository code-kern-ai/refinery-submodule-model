from datetime import datetime
from typing import Dict, List, Optional, Tuple
from src.model.data_objects import general
from src.model.session import session
from submodules.model.models import CognitionProject
from src.model import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(project_id: str) -> CognitionProject:
    return (
        session.query(CognitionProject)
        .filter(CognitionProject.id == project_id)
        .first()
    )


def create(
    name: str,
    description: str,
    org_id: str,
    user_id: str,
    with_commit: bool = True,
    timestamp: Optional[str] = None,
) -> CognitionProject:
    project: CognitionProject = CognitionProject(
        name=name,
        description=description,
        organization_id=org_id,
        created_by=user_id,
        timestamp=timestamp,
    )
    general.add(project, with_commit)
    return project
