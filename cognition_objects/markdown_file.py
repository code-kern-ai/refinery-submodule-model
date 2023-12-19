from typing import List, Optional, Tuple
from datetime import datetime

from .. import enums
from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownFile, CognitionMarkdownLLMLogs


def get(org_id: str, md_file_id: str) -> CognitionMarkdownFile:
    return (
        session.query(CognitionMarkdownFile)
        .filter(
            CognitionMarkdownFile.organization_id == org_id,
            CognitionMarkdownFile.id == md_file_id,
        )
        .first()
    )


def get_all_for_dataset_id(
    org_id: str,
    dataset_id: str,
    only_finished: bool,
    only_reviewed: bool,
) -> List[CognitionMarkdownFile]:
    query = session.query(CognitionMarkdownFile).filter(
        CognitionMarkdownFile.organization_id == org_id,
        CognitionMarkdownFile.dataset_id == dataset_id,
    )

    if only_finished:
        query = query.filter(
            CognitionMarkdownFile.state
            == enums.CognitionMarkdownFileState.FINISHED.value
        )

    if only_reviewed:
        query = query.filter(CognitionMarkdownFile.is_reviewed == True)

    query = query.order_by(CognitionMarkdownFile.created_at.asc())
    return query.all()


def get_all_paginated_for_dataset(
    org_id: str,
    dataset_id: str,
    page: int,
    limit: int,
) -> Tuple[int, int, List[CognitionMarkdownFile]]:
    total_count = (
        session.query(CognitionMarkdownFile.id)
        .filter(CognitionMarkdownFile.organization_id == org_id)
        .filter(CognitionMarkdownFile.dataset_id == dataset_id)
        .count()
    )

    num_pages = int(total_count / limit)
    if total_count % limit > 0:
        num_pages += 1

    query_results = (
        session.query(CognitionMarkdownFile)
        .filter(CognitionMarkdownFile.organization_id == org_id)
        .filter(CognitionMarkdownFile.dataset_id == dataset_id)
        .order_by(CognitionMarkdownFile.created_at.asc())
        .limit(limit)
        .offset((page - 1) * limit)
        .all()
    )

    return total_count, num_pages, query_results


def get_all_logs_for_md_file_id(md_file_id: str) -> List[CognitionMarkdownLLMLogs]:
    return (
        session.query(CognitionMarkdownLLMLogs)
        .filter(CognitionMarkdownLLMLogs.markdown_file_id == md_file_id)
        .order_by(CognitionMarkdownLLMLogs.created_at.asc())
        .all()
    )


def create(
    org_id: str,
    dataset_id: str,
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
        dataset_id=dataset_id,
        created_by=created_by,
        created_at=created_at,
        file_name=file_name,
        content=content,
        error=error,
        category_origin=category_origin,
        state=enums.CognitionMarkdownFileState.QUEUE.value,
    )
    general.add(markdown_file, with_commit)

    return markdown_file


def update(
    org_id: str,
    markdown_file_id: str,
    content: Optional[str] = None,
    is_reviewed: Optional[bool] = None,
    state: Optional[str] = None,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    error: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionMarkdownFile:
    markdown_file: CognitionMarkdownFile = get(org_id, markdown_file_id)
    if content is not None:
        markdown_file.content = content
    if is_reviewed is not None:
        markdown_file.is_reviewed = is_reviewed
    if state is not None:
        markdown_file.state = state
    if started_at is not None:
        markdown_file.started_at = started_at
    if finished_at is not None:
        markdown_file.finished_at = finished_at
    if error is not None:
        markdown_file.error = error

    general.flush_or_commit(with_commit)

    return markdown_file


def create_md_llm_log(
    markdown_file_id: str,
    model_used: str,
    input_text: str,
    output_text: Optional[str] = None,
    error: Optional[str] = None,
    created_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    with_commit: bool = True,
) -> None:
    md_llm_log = CognitionMarkdownLLMLogs(
        markdown_file_id=markdown_file_id,
        input=input_text,
        output=output_text,
        error=error,
        created_at=created_at,
        finished_at=finished_at,
        model_used=model_used,
    )
    general.add(md_llm_log, with_commit)


def delete(org_id: str, md_file_id: str, with_commit: bool = True) -> None:
    session.query(CognitionMarkdownFile).filter(
        CognitionMarkdownFile.organization_id == org_id,
        CognitionMarkdownFile.id == md_file_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_many(org_id: str, md_file_ids: List[str], with_commit: bool = True) -> None:
    session.query(CognitionMarkdownFile).filter(
        CognitionMarkdownFile.organization_id == org_id,
        CognitionMarkdownFile.id.in_(md_file_ids),
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)
