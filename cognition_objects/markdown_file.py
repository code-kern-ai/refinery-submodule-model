from typing import List, Optional, Tuple
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownFile


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
        session.query(CognitionMarkdownFile.id)
        .filter(CognitionMarkdownFile.organization_id == org_id)
        .count()
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


def get_all_reviewed_for_category_origin(
    org_id: str,
    category_origin: str,
) -> List[CognitionMarkdownFile]:
    return (
        session.query(CognitionMarkdownFile)
        .filter(CognitionMarkdownFile.organization_id == org_id)
        .filter(CognitionMarkdownFile.category_origin == category_origin)
        .filter(CognitionMarkdownFile.is_reviewed == True)
        .order_by(CognitionMarkdownFile.created_at.asc())
        .all()
    )


def create(
    org_id: str,
    created_by: str,
    file_name: str,
    category_origin: str,
    content: Optional[str] = None,
    error: Optional[str] = None,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionMarkdownFile:
    markdown_file: CognitionMarkdownFile = CognitionMarkdownFile(
        organization_id=org_id,
        created_by=created_by,
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
    is_reviewed: Optional[bool] = None,
    with_commit: bool = True,
) -> CognitionMarkdownFile:
    markdown_file: CognitionMarkdownFile = get(markdown_file_id)
    if content is not None:
        markdown_file.content = content
    if is_reviewed is not None:
        markdown_file.is_reviewed = is_reviewed

    general.flush_or_commit(with_commit)

    return markdown_file


def delete(md_file_id: str, with_commit: bool = True) -> None:
    session.query(CognitionMarkdownFile).filter(
        CognitionMarkdownFile.id == md_file_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_many(md_file_ids: List[str], with_commit: bool = True) -> None:
    session.query(CognitionMarkdownFile).filter(
        CognitionMarkdownFile.id.in_(md_file_ids),
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)
