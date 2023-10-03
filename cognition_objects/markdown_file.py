from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..cognition_objects import message
from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownFile
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(md_file_id: str) -> CognitionMarkdownFile:
    return (
        session.query(CognitionMarkdownFile)
        .filter(CognitionMarkdownFile.id == md_file_id)
        .first()
    )


def get_all_paginated_for_category_origin(
    org_id: str,
    category_origin: str,
    page: int,
    limit: int,
) -> Tuple[int, int, List[CognitionMarkdownFile]]:
    total_count = (
        session.query(func.count(CognitionMarkdownFile.id))
        .filter(CognitionMarkdownFile.organization_id == org_id)
        .scalar()
    )

    num_pages = int(total_count / limit)
    if total_count % limit > 0:
        num_pages += 1

    query_results = (
        session.query(CognitionMarkdownFile)
        .filter(CognitionMarkdownFile.organization_id == org_id)
        .filter(CognitionMarkdownFile.category_origin == category_origin)
        .order_by(CognitionMarkdownFile.created_at.asc())
        .limit(limit)
        .offset((page - 1) * limit)
        .all()
    )

    return total_count, num_pages, query_results


def create(
    org_id: str,
    user_id: str,
    file_name: str,
    category_origin: str,
    content: Optional[str] = None,
    error: Optional[str] = None,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> CognitionMarkdownFile:
    markdown_file: CognitionMarkdownFile = CognitionMarkdownFile(
        organization_id=org_id,
        user_id=user_id,
        created_at=created_at,
        file_name=file_name,
        content=content,
        error=error,
        category_origin=category_origin,
    )
    general.add(markdown_file, with_commit)

    return markdown_file


def update(
    markdown_file_id: str,
    content: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionMarkdownFile:
    markdown_file: CognitionMarkdownFile = get(markdown_file_id)
    if content is not None:
        markdown_file.content = content

    general.flush_or_commit(with_commit)

    return markdown_file


def delete(md_file_id: str, with_commit: bool = True) -> None:
    session.query(CognitionMarkdownFile).filter(
        CognitionMarkdownFile.id == md_file_id,
    ).delete()
    general.flush_or_commit(with_commit)
