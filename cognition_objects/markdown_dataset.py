from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownDataset, Project
from ..enums import Tablenames, MarkdownFileCategoryOrigin
from ..util import prevent_sql_injection


def get(org_id: str, id: str) -> CognitionMarkdownDataset:
    return (
        session.query(CognitionMarkdownDataset)
        .filter(
            CognitionMarkdownDataset.organization_id == org_id,
            CognitionMarkdownDataset.id == id,
        )
        .first()
    )


def __get_enriched_query(
    org_id: str,
    id: Optional[str] = None,
    category_origin: Optional[str] = None,
    query_add: Optional[str] = "",
) -> str:
    where_add = ""
    if id:
        id = prevent_sql_injection(id)
        where_add += f" AND md.id = '{id}'"
    elif category_origin:
        where_add += f" AND md.category_origin = '{category_origin}'"
    org_id = prevent_sql_injection(org_id)
    return f"""
        SELECT md.*, COALESCE(mf.num_files, 0) AS num_files, COALESCE(mf.num_reviewed_files, 0) AS num_reviewed_files
        FROM cognition.{Tablenames.MARKDOWN_DATASET.value} md
        LEFT JOIN (
            SELECT dataset_id, COUNT(*) as num_files, COUNT(CASE WHEN is_reviewed = TRUE THEN 1 END) AS num_reviewed_files
            FROM cognition.{Tablenames.MARKDOWN_FILE.value}
            GROUP BY dataset_id
        ) mf ON md.id = mf.dataset_id
        WHERE md.organization_id = '{org_id}' {where_add}
        {query_add}
    """


def get_enriched(org_id: str, id: str) -> Dict[str, Any]:
    query = __get_enriched_query(org_id=org_id, id=id)
    return general.execute_first(query)


def get_all_paginated_for_category_origin(
    org_id: str,
    category_origin: str,
    page: int,
    limit: int,
) -> Tuple[int, int, List[CognitionMarkdownDataset]]:
    total_count = (
        session.query(CognitionMarkdownDataset.id)
        .filter(CognitionMarkdownDataset.organization_id == org_id)
        .filter(CognitionMarkdownDataset.category_origin == category_origin)
        .count()
    )

    num_pages = int(total_count / limit)
    if total_count % limit > 0:
        num_pages += 1

    query_add = f"""
        ORDER BY md.created_at
        LIMIT {limit}
        OFFSET {(page - 1) * limit}
    """
    enriched_query = __get_enriched_query(
        org_id=org_id, category_origin=category_origin, query_add=query_add
    )
    query_results = general.execute_all(enriched_query)

    return total_count, num_pages, query_results


def get_dataset_count_dict(org_id: str) -> Dict[str, int]:
    # no need to access with get since all possible values are known (or at least should be)
    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))

    option_select = "' as category \nUNION ALL\nSELECT '".join(
        [e.value for e in MarkdownFileCategoryOrigin]
    )
    option_select = "SELECT '" + option_select + "'"

    query = f"""
    SELECT jsonb_object_agg(category,c)
    FROM (
        SELECT category, COALESCE(COUNT(md.id),0) c
        FROM (
            {option_select} ) o
        LEFT JOIN cognition.markdown_dataset md
            ON o.category = md.category_origin AND md.organization_id = '{org_id}'
        GROUP BY category
    )x
    """
    result = general.execute_first(query)
    if result and result[0]:
        return result[0]
    raise Exception("No results found")


def create(
    org_id: str,
    created_by: str,
    category_origin: str,
    name: str,
    description: str,
    tokenizer: str,
    refinery_project_id: str,
    environment_variable_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionMarkdownDataset:
    new_dataset = CognitionMarkdownDataset(
        organization_id=org_id,
        refinery_project_id=refinery_project_id,
        environment_variable_id=environment_variable_id,
        created_by=created_by,
        category_origin=category_origin,
        name=name,
        description=description,
        tokenizer=tokenizer,
        created_at=created_at,
    )

    general.add(new_dataset, with_commit)

    return new_dataset


def update(
    org_id: str,
    dataset_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionMarkdownDataset:
    dataset = get(org_id, dataset_id)

    if name:
        dataset.name = name

    if description:
        dataset.description = description

    general.flush_or_commit(with_commit)

    return dataset


def delete_many(org_id: str, dataset_ids: List[str], with_commit: bool = True) -> None:
    session.query(Project).filter(
        Project.organization_id == org_id,
        Project.id.in_(
            session.query(CognitionMarkdownDataset.refinery_project_id).filter(
                CognitionMarkdownDataset.organization_id == org_id,
                CognitionMarkdownDataset.id.in_(dataset_ids),
                CognitionMarkdownDataset.refinery_project_id.isnot(None),
            )
        ),
    ).delete(synchronize_session=False)

    session.query(CognitionMarkdownDataset).filter(
        CognitionMarkdownDataset.organization_id == org_id,
        CognitionMarkdownDataset.id.in_(dataset_ids),
    ).delete(synchronize_session=False)

    general.flush_or_commit(with_commit)
