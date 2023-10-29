from typing import List, Optional, Dict, Any
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionRetriever, CognitionRetrieverPart


def get(retriever_id: str) -> CognitionRetriever:
    return (
        session.query(CognitionRetriever)
        .filter(CognitionRetriever.id == retriever_id)
        .first()
    )


def get_part(retriever_part_id: str) -> CognitionRetrieverPart:
    return (
        session.query(CognitionRetrieverPart)
        .filter(CognitionRetrieverPart.id == retriever_part_id)
        .first()
    )


def get_all_parts_by_retriever_id(retriever_id: str) -> List[CognitionRetrieverPart]:
    return (
        session.query(CognitionRetrieverPart)
        .filter(CognitionRetrieverPart.retriever_id == retriever_id)
        .order_by(CognitionRetrieverPart.created_at)
        .all()
    )


def get_by_project_id_and_strategy_step_id(
    project_id: str, strategy_step_id: str
) -> CognitionRetriever:
    return (
        session.query(CognitionRetriever)
        .filter(
            CognitionRetriever.project_id == project_id,
            CognitionRetriever.strategy_step_id == strategy_step_id,
        )
        .first()
    )


def create(
    project_id: str,
    strategy_step_id: str,
    user_id: str,
    search_input_field: str,
    meta_data: Dict[str, Any],
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionRetriever:
    retriever: CognitionRetriever = CognitionRetriever(
        project_id=project_id,
        strategy_step_id=strategy_step_id,
        created_by=user_id,
        created_at=created_at,
        search_input_field=search_input_field,
        meta_data=meta_data,
    )
    general.add(retriever, with_commit)

    return retriever


def create_part(
    retriever_id: str,
    embedding_name: str,
    number_records: int,
    enabled: bool,
    user_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionRetrieverPart:
    retriever_part: CognitionRetrieverPart = CognitionRetrieverPart(
        retriever_id=retriever_id,
        embedding_name=embedding_name,
        number_records=number_records,
        enabled=enabled,
        created_by=user_id,
        created_at=created_at,
    )
    general.add(retriever_part, with_commit)

    return retriever_part


def update(
    retriever_id: str,
    meta_data: Optional[Dict[str, Any]] = None,
    search_input_field: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionRetriever:
    retriever: CognitionRetriever = get(retriever_id)
    if meta_data is not None:
        retriever.meta_data = meta_data
    if search_input_field is not None:
        retriever.search_input_field = search_input_field
    general.flush_or_commit(with_commit)
    return retriever


def update_part(
    retriever_part_id: str,
    number_records: Optional[int] = None,
    enabled: Optional[bool] = None,
    with_commit: bool = True,
) -> CognitionRetrieverPart:
    retriever_part: CognitionRetrieverPart = get_part(retriever_part_id)
    if number_records:
        retriever_part.number_records = number_records
    if enabled is not None:
        retriever_part.enabled = enabled
    general.flush_or_commit(with_commit)
    return retriever_part


def delete(retriever_id: str, with_commit: bool = True) -> None:
    session.query(CognitionRetriever).filter(
        CognitionRetriever.id == retriever_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_part(retriever_part_id: str, with_commit: bool = True) -> None:
    session.query(CognitionRetrieverPart).filter(
        CognitionRetrieverPart.id == retriever_part_id,
    ).delete()
    general.flush_or_commit(with_commit)
