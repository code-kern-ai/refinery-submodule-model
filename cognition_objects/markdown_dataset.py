from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownDataset, CognitionMarkdownFile, Project
from ..enums import Tablenames, MarkdownFileCategoryOrigin
from ..util import prevent_sql_injection
from .markdown_file import delete_many as delete_many_md_files


def get(org_id: str, id: str) -> CognitionMarkdownDataset:
    return (
        session.query(CognitionMarkdownDataset)
        .filter(
            CognitionMarkdownDataset.organization_id == org_id,
            CognitionMarkdownDataset.id == id,
        )
        .first()
    )


def get_all(org_id: str) -> CognitionMarkdownDataset:
    return (
        session.query(CognitionMarkdownDataset)
        .filter(CognitionMarkdownDataset.organization_id == org_id)
        .all()
    )


def __get_enriched_query(
    org_id: str,
    id: Optional[str] = None,
    category_origin: Optional[str] = None,
    md_file_name_contains: Optional[str] = None,
    query_add: Optional[str] = "",
) -> str:
    where_add = ""
    if id:
        id = prevent_sql_injection(id, isinstance(id, str))
        where_add += f" AND md.id = '{id}'"
    elif category_origin:
        where_add += f" AND md.category_origin = '{category_origin}'"
    if md_file_name_contains:
        md_file_name_contains = prevent_sql_injection(
            md_file_name_contains, isinstance(md_file_name_contains, str)
        )
        where_add += f"""
            AND EXISTS (
                SELECT 1
                FROM cognition.{Tablenames.MARKDOWN_FILE.value} mf_filter
                WHERE mf_filter.organization_id = md.organization_id
                AND mf_filter.dataset_id = md.id
                AND mf_filter.file_name ILIKE '%{md_file_name_contains}%'
            )
        """
    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))
    return f"""
        SELECT 
            md.*, 
            COALESCE(mf.num_files, 0) AS num_files, 
            COALESCE(mf.num_reviewed_files, 0) AS num_reviewed_files, 
            ecp.etl_config,
            ecp.id as etl_config_id
        FROM cognition.{Tablenames.MARKDOWN_DATASET.value} md
        LEFT JOIN (
            SELECT dataset_id, COUNT(*) as num_files, COUNT(CASE WHEN is_reviewed = TRUE THEN 1 END) AS num_reviewed_files
            FROM cognition.{Tablenames.MARKDOWN_FILE.value}
            GROUP BY dataset_id
        ) mf ON md.id = mf.dataset_id
        LEFT JOIN(
            SELECT md.id, json_array_elements(md.useable_etl_configurations) config_ids 
            FROM cognition.{Tablenames.MARKDOWN_DATASET.value} md
        ) ed ON ed.id = md.id AND (ed.config_ids->>'isDefault')::bool is true
        LEFT JOIN(
            SELECT ecp.id, ecp.etl_config
            FROM cognition.{Tablenames.ETL_CONFIG_PRESET.value} ecp
        ) ecp on ecp.id = (ed.config_ids ->> 'id')::uuid
        WHERE md.organization_id = '{org_id}' {where_add}
        {query_add}
    """


def get_enriched(org_id: str, id: str) -> Dict[str, Any]:
    query = __get_enriched_query(org_id=org_id, id=id)
    return general.execute_first(query)


_DATASET_LIST_SORT_SQL = {
    "created_at": "md.created_at",
    "name": "md.name",
    "description": "md.description",
    "num_files": "COALESCE(mf.num_files, 0)",
    "num_reviewed_files": "COALESCE(mf.num_reviewed_files, 0)",
}


def __dataset_list_order_sql(
    sort_by: Optional[str], sort_direction: Optional[str]
) -> str:
    raw = (sort_by or "").strip().lower()
    field = raw if raw in _DATASET_LIST_SORT_SQL else "created_at"
    col_sql = _DATASET_LIST_SORT_SQL[field]
    if sort_direction and str(sort_direction).strip().upper() == "ASC":
        direction = "ASC"
    else:
        direction = "DESC"
    return f"ORDER BY {col_sql} {direction}, md.id {direction}"


def get_all_paginated_for_category_origin(
    org_id: str,
    page: int,
    limit: int,
    category_origin: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_direction: Optional[str] = None,
    md_file_name_contains: Optional[str] = None,
) -> Tuple[int, int, List[CognitionMarkdownDataset]]:
    total_count_query = session.query(CognitionMarkdownDataset.id).filter(
        CognitionMarkdownDataset.organization_id == org_id
    )
    if category_origin is not None:
        total_count_query = total_count_query.filter(
            CognitionMarkdownDataset.category_origin == category_origin
        )
    if md_file_name_contains:
        total_count_query = total_count_query.filter(
            session.query(CognitionMarkdownFile.id)
            .filter(
                CognitionMarkdownFile.organization_id == org_id,
                CognitionMarkdownFile.dataset_id == CognitionMarkdownDataset.id,
                CognitionMarkdownFile.file_name.ilike(f"%{md_file_name_contains}%"),
            )
            .exists()
        )
    total_count = total_count_query.count()

    num_pages = int(total_count / limit)
    if total_count % limit > 0:
        num_pages += 1

    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))
    category_origin = prevent_sql_injection(
        category_origin, isinstance(category_origin, str)
    )
    md_file_name_contains = prevent_sql_injection(
        md_file_name_contains, isinstance(md_file_name_contains, str)
    )
    limit = prevent_sql_injection(limit, isinstance(limit, int))
    page = prevent_sql_injection(page, isinstance(page, int))

    query_add = f"""
        {__dataset_list_order_sql(sort_by, sort_direction)}
        LIMIT {limit}
        OFFSET {(page - 1) * limit}
    """
    enriched_query = __get_enriched_query(
        org_id=org_id,
        category_origin=category_origin,
        md_file_name_contains=md_file_name_contains,
        query_add=query_add,
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


def get_default_etl_config_id(org_id: str, dataset_id: str) -> Optional[str]:
    dataset = get(org_id, dataset_id)
    if dataset and dataset.useable_etl_configurations:
        for config in dataset.useable_etl_configurations:
            if config.get("isDefault"):
                return config.get("id")
    raise ValueError(f"No default ETL config found for dataset {dataset_id}")


def create(
    org_id: str,
    created_by: str,
    category_origin: str,
    name: str,
    description: str,
    refinery_project_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    useable_etl_configurations: Optional[List[Dict[str, Any]]] = None,
) -> CognitionMarkdownDataset:
    new_dataset = CognitionMarkdownDataset(
        organization_id=org_id,
        refinery_project_id=refinery_project_id,
        created_by=created_by,
        category_origin=category_origin,
        name=name,
        description=description,
        created_at=created_at,
        useable_etl_configurations=useable_etl_configurations,
    )

    general.add(new_dataset, with_commit)

    return new_dataset


def update(
    org_id: str,
    dataset_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    useable_etl_configurations: Optional[List[Dict[str, Any]]] = None,
    with_commit: bool = True,
) -> CognitionMarkdownDataset:
    dataset = get(org_id, dataset_id)

    if name:
        dataset.name = name

    if description:
        dataset.description = description

    if useable_etl_configurations:
        dataset.useable_etl_configurations = useable_etl_configurations

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

    md_file_ids = (
        session.query(CognitionMarkdownFile.id)
        .filter(
            CognitionMarkdownFile.organization_id == org_id,
            CognitionMarkdownFile.dataset_id.in_(dataset_ids),
        )
        .all()
    )

    delete_many_md_files(
        org_id=org_id,
        md_file_ids=[md_file_id for (md_file_id,) in md_file_ids],
        with_commit=True,
    )

    session.query(CognitionMarkdownDataset).filter(
        CognitionMarkdownDataset.organization_id == org_id,
        CognitionMarkdownDataset.id.in_(dataset_ids),
    ).delete(synchronize_session=False)

    general.flush_or_commit(with_commit)
