from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

from .. import enums
from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownFile
from ..util import prevent_sql_injection


def get(org_id: str, md_file_id: str) -> CognitionMarkdownFile:
    return (
        session.query(CognitionMarkdownFile)
        .filter(
            CognitionMarkdownFile.organization_id == org_id,
            CognitionMarkdownFile.id == md_file_id,
        )
        .first()
    )


def get_enriched(org_id: str, md_file_id: str) -> Dict[str, Any]:
    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))
    md_file_id = prevent_sql_injection(md_file_id, isinstance(org_id, str))
    enriched_query = __get_enriched_query(org_id=org_id, md_file_id=md_file_id)
    return general.execute_first(enriched_query)


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


def __get_enriched_query(
    org_id: str,
    md_file_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    query_add: Optional[str] = "",
    exclude_content: bool = False,
) -> str:
    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))
    where_add = ""
    if md_file_id:
        md_file_id = prevent_sql_injection(md_file_id, isinstance(md_file_id, str))
        where_add += f" AND mf.id = '{md_file_id}'"
    if dataset_id:
        prevent_sql_injection(dataset_id, isinstance(dataset_id, str))
        where_add += f" AND mf.dataset_id = '{dataset_id}'"
    if exclude_content:
        mf_select = general.construct_select_columns(
            "markdown_file", "cognition", prefix="mf", exclude_columns=["content"]
        )
    else:
        mf_select = "mf.*"

    query = f"""SELECT {mf_select} FROM cognition.markdown_file mf
    """
    query += f"WHERE mf.organization_id = '{org_id}' {where_add}"
    query += query_add
    return query


def get_all_paginated_for_dataset(
    org_id: str,
    dataset_id: str,
    page: int,
    limit: int,
    exclude_content: bool,
    only_count_llm_logs: bool,
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

    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))
    dataset_id = prevent_sql_injection(dataset_id, isinstance(org_id, str))
    limit = prevent_sql_injection(limit, isinstance(limit, int))
    page = prevent_sql_injection(page, isinstance(page, int))
    query_add = f"""
    ORDER BY mf.created_at DESC
    LIMIT {limit}
    OFFSET {(page - 1) * limit}
    """
    enriched_query = __get_enriched_query(
        org_id=org_id,
        dataset_id=dataset_id,
        query_add=query_add,
        exclude_content=exclude_content,
    )
    query_results = general.execute_all(enriched_query)

    return total_count, num_pages, query_results


def can_access_file(org_id: str, file_id: str) -> bool:
    # since org specific files dont have a project_id but we still need to check the access rights
    # we collect from the requested file and match with org id from middleware/internal routing

    q = session.query(CognitionMarkdownFile.organization_id).filter(
        CognitionMarkdownFile.id == file_id,
        CognitionMarkdownFile.organization_id == org_id,
    )
    return session.query(q.exists()).scalar()


def create(
    org_id: str,
    dataset_id: str,
    created_by: str,
    file_name: str,
    category_origin: str,
    content: Optional[str] = None,
    error: Optional[str] = None,
    meta_data: Optional[Dict[str, Any]] = None,
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
        meta_data=meta_data,
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
    meta_data: Optional[Dict[str, Any]] = None,
    overwrite_meta_data: bool = True,
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
    if meta_data is not None:
        if overwrite_meta_data:
            markdown_file.meta_data = meta_data
        else:
            markdown_file.meta_data = {**markdown_file.meta_data, **meta_data}
    general.flush_or_commit(with_commit)

    return markdown_file


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
