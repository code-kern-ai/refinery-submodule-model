from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..cognition_objects import message
from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownFileChunk
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get_all_by_markdown_file(md_file_id: str) -> List[CognitionMarkdownFileChunk]:
    return (
        session.query(CognitionMarkdownFileChunk)
        .filter(CognitionMarkdownFileChunk.markdown_file_id == md_file_id)
        .all()
    )


def create(
    user_id: str,
    md_file_id: str,
    start_index: int,
    end_index: int,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> CognitionMarkdownFileChunk:
    strategy: CognitionMarkdownFileChunk = CognitionMarkdownFileChunk(
        markdown_file_id=md_file_id,
        start_index=start_index,
        end_index=end_index,
        created_at=created_at,
    )
    general.add(strategy, with_commit)

    return strategy
