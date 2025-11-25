from typing import Any, List, Optional, Dict
from ..business_objects import general
from ..session import session
from ..models import (
    ETLConfigPresets,
)


def get(config_id: str) -> ETLConfigPresets:
    return (
        session.query(ETLConfigPresets)
        .filter(
            ETLConfigPresets.id == config_id,
        )
        .first()
    )


def get_all_in_org(
    org_id: str,
) -> List[ETLConfigPresets]:

    return (
        session.query(ETLConfigPresets)
        .filter(
            ETLConfigPresets.organization_id == org_id,
        )
        .order_by(ETLConfigPresets.created_at.asc())
        .all()
    )


def create(
    org_id: str,
    user_id: str,
    name: str,
    description: str,
    etl_config: Dict[str, Any],
    add_config: Dict[str, Any],
    with_commit: bool = True,
) -> ETLConfigPresets:
    etl_config: ETLConfigPresets = ETLConfigPresets(
        created_by=user_id,
        organization_id=org_id,
        name=name,
        description=description,
        etl_config=etl_config,
        add_config=add_config,
    )
    general.add(etl_config, with_commit)
    return etl_config


def update(
    org_id: str,
    etl_config_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    etl_config: Optional[Dict[str, Any]] = None,
    add_config: Optional[Dict[str, Any]] = None,
    with_commit: bool = True,
) -> ETLConfigPresets:
    etl_config_item: ETLConfigPresets = get(etl_config_id)
    if not etl_config_item or str(etl_config_item.organization_id) != org_id:
        raise Exception("ETL Config not found")

    if name is not None:
        etl_config_item.name = name
    if description is not None:
        etl_config_item.description = description
    if etl_config is not None:
        etl_config_item.etl_config = etl_config
    if add_config is not None:
        etl_config_item.add_config = add_config
    general.flush_or_commit(with_commit)
    return etl_config_item


def delete(org_id: str, etl_config_id: str, with_commit: bool = True) -> None:
    session.query(ETLConfigPresets).filter(
        ETLConfigPresets.organization_id == org_id,
        ETLConfigPresets.id == etl_config_id,
    ).delete()
    general.flush_or_commit(with_commit)
