from typing import List, Optional, Tuple
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionMarkdownDataset, Project


def get(org_id: str, id: str) -> CognitionMarkdownDataset:
    return (
        session.query(CognitionMarkdownDataset)
        .filter(
            CognitionMarkdownDataset.organization_id == org_id,
            CognitionMarkdownDataset.id == id,
        )
        .first()
    )


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

    query_results = (
        session.query(CognitionMarkdownDataset)
        .filter(CognitionMarkdownDataset.organization_id == org_id)
        .filter(CognitionMarkdownDataset.category_origin == category_origin)
        .order_by(CognitionMarkdownDataset.created_at.asc())
        .limit(limit)
        .offset((page - 1) * limit)
        .all()
    )

    return total_count, num_pages, query_results


def create(
    org_id: str,
    created_by: str,
    category_origin: str,
    name: str,
    description: str,
    tokenizer: str,
    refinery_project_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionMarkdownDataset:
    new_dataset = CognitionMarkdownDataset(
        organization_id=org_id,
        refinery_project_id=refinery_project_id,
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


def delete(org_id: str, dataset_id: str, with_commit: bool = True) -> None:
    session.query(CognitionMarkdownDataset).filter(
        CognitionMarkdownDataset.organization_id == org_id,
        CognitionMarkdownDataset.id == dataset_id,
    ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)


def delete_many(org_id: str, dataset_ids: List[str], with_commit: bool = True) -> None:
    datasets = (
        session.query(CognitionMarkdownDataset)
        .filter(
            CognitionMarkdownDataset.organization_id == org_id,
            CognitionMarkdownDataset.id.in_(dataset_ids),
        )
        .all()
    )

    for dataset in datasets:
        session.query(Project).filter(
            Project.organization_id == org_id, Project.id == dataset.refinery_project_id
        ).delete(synchronize_session=False)
    general.flush_or_commit(with_commit)
