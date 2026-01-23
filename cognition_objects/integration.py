from typing import List, Optional, Dict, Union, Any
import datetime
from sqlalchemy import func
from sqlalchemy.orm.attributes import flag_modified


from ..business_objects import general
from ..integration_objects import manager as integration_records_bo
from ..session import session
from ..models import CognitionIntegration, CognitionGroup, EtlTask
from ..enums import CognitionIntegrationType, CognitionIntegrationState
from ..util import prevent_sql_injection
from submodules.model import enums

FINISHED_STATES = [
    CognitionIntegrationState.FINISHED.value,
    CognitionIntegrationState.ETL_PROCESSING.value,
]


def get_by_ids(ids: List[str]) -> List[CognitionIntegration]:
    return (
        session.query(CognitionIntegration)
        .filter(CognitionIntegration.id.in_(ids))
        .all()
    )


def get_by_id(id: str) -> CognitionIntegration:
    return (
        session.query(CognitionIntegration)
        .filter(CognitionIntegration.id == id)
        .first()
    )


def get_all(
    integration_type: Optional[str] = None,
    exclude_failed: bool = False,
    only_synced: bool = False,
) -> List[CognitionIntegration]:
    query = session.query(CognitionIntegration)
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    if exclude_failed:
        query = query.filter(
            CognitionIntegration.state != CognitionIntegrationState.FAILED.value
        )
    if only_synced:
        query = query.filter(CognitionIntegration.is_synced == True)
    return query.order_by(CognitionIntegration.created_at.desc()).all()


def get_all_all_overview(
    integration_type: Optional[CognitionIntegrationType] = None,
) -> Dict[str, Any]:
    add_filter = ""
    if integration_type:
        add_filter = f" WHERE type = '{integration_type.value}' "
    query = f"""
    SELECT jsonb_object_agg(org_id::text, integrations)
    FROM (
        SELECT 
            organization_id AS org_id,
            jsonb_object_agg(
                id::text,
                jsonb_build_object(
                    'name', name,
                    'state', state,
                    'error_message', error_message,
                    'is_synced', is_synced,
                    'last_synced_at', last_synced_at
                )
            ) AS integrations
        FROM cognition.integration i
        {add_filter}
        GROUP BY organization_id
    ) sub """
    value = general.execute_first(query)
    if value and value[0]:
        return value[0]
    return {}


def get_all_in_org(
    org_id: str,
    integration_type: Optional[str] = None,
    only_synced: bool = False,
    exclude_failed: bool = False,
) -> List[CognitionIntegration]:
    query = session.query(CognitionIntegration).filter(
        CognitionIntegration.organization_id == org_id
    )
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    if only_synced:
        query = query.filter(CognitionIntegration.is_synced == True)
    if exclude_failed:
        query = query.filter(
            CognitionIntegration.state != CognitionIntegrationState.FAILED.value
        )
    return query.order_by(CognitionIntegration.created_at.desc()).all()


def get_all_in_org_paginated(
    org_id: str,
    integration_type: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
) -> List[CognitionIntegration]:
    query = session.query(CognitionIntegration).filter(
        CognitionIntegration.organization_id == org_id,
    )

    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)

    return (
        query.order_by(CognitionIntegration.created_at.desc())
        .limit(page_size)
        .offset(max(0, (page - 1) * page_size))
        .all()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionIntegration]:
    return (
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.project_id == project_id,
        )
        .order_by(CognitionIntegration.created_at.desc())
        .all()
    )


def get_last_synced_at(
    org_id: str, integration_type: Optional[str] = None
) -> datetime.datetime:
    query = session.query(func.max(CognitionIntegration.last_synced_at)).filter(
        CognitionIntegration.organization_id == org_id
    )
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    result = query.first()
    return result[0] if result else None


def get_active_etl_tasks(
    integration_id: str,
) -> List[EtlTask]:
    IntegrationModel = integration_records_bo.integration_model(integration_id)
    return (
        session.query(EtlTask)
        .filter(EtlTask.is_active == True)
        .join(
            IntegrationModel,
            (EtlTask.id == IntegrationModel.etl_task_id)
            & (IntegrationModel.integration_id == integration_id),
        )
        .all()
    )


def get_all_etl_tasks(
    integration_id: str,
) -> List[EtlTask]:
    IntegrationModel = integration_records_bo.integration_model(integration_id)
    return (
        session.query(EtlTask)
        .join(
            IntegrationModel,
            (IntegrationModel.etl_task_id == EtlTask.id)
            & (IntegrationModel.integration_id == integration_id),
        )
        .all()
    )


# TODO change this funciton for cognition-gateway
def get_integration_progress(
    integration_id: str,
    by: Optional[str] = None,
    scope: Optional[str] = enums.IntegrationRecordScope.ROOT.value,
) -> float:

    integration = get_by_id(integration_id)
    count_all_records = integration_records_bo.count(integration, by, scope)

    if (
        count_all_records == 0
        or integration.state == enums.CognitionIntegrationState.FAILED.value
    ):

        return 0.0

    all_tasks = get_all_etl_tasks(integration_id)

    finished_tasks = [task for task in all_tasks if task.state in FINISHED_STATES]
    count_finished_tasks = len(finished_tasks)

    # backward compatibility
    if not all_tasks or len(all_tasks) != count_all_records:
        all_records, _ = integration_records_bo.get_all_by_integration_id(
            integration_id
        )
        additional_finished = len(
            [record for record in all_records if not record.etl_task_id]
        )
        count_finished_tasks += additional_finished

    integration_progress = round((count_finished_tasks / count_all_records) * 100.0, 2)
    if integration.state not in FINISHED_STATES:
        integration_progress = min(integration_progress - 1, 0)
    return integration_progress


def count_org_integrations(org_id: str) -> Dict[str, int]:
    counts = (
        session.query(CognitionIntegration.type, func.count(CognitionIntegration.id))
        .filter(
            CognitionIntegration.organization_id == org_id,
        )
        .group_by(CognitionIntegration.type)
        .all()
    )
    return {cognition_type: count for cognition_type, count in counts}


def create(
    org_id: str,
    user_id: str,
    name: str,
    description: str,
    tokenizer: str,
    state: str,
    integration_type: CognitionIntegrationType,
    integration_config: Dict,
    llm_config: Dict,
    started_at: Optional[datetime.datetime] = None,
    created_at: Optional[datetime.datetime] = None,
    finished_at: Optional[datetime.datetime] = None,
    id: Optional[str] = None,
    project_id: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionIntegration:
    integration: CognitionIntegration = CognitionIntegration(
        id=id,
        organization_id=org_id,
        project_id=project_id,
        created_by=user_id,
        updated_by=user_id,
        created_at=created_at,
        started_at=started_at,
        finished_at=finished_at,
        name=name,
        description=description,
        tokenizer=tokenizer,
        state=state,
        type=integration_type.value,
        config=integration_config,
        llm_config=llm_config,
        delta_criteria={"delta_url": None},
    )
    general.add(integration, with_commit)

    return integration


def update(
    id: str,
    project_id: Optional[str] = None,
    updated_by: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tokenizer: Optional[str] = None,
    state: Optional[CognitionIntegrationState] = None,
    integration_config: Optional[int] = None,
    llm_config: Optional[Dict] = None,
    error_message: Optional[str] = None,
    started_at: Optional[datetime.datetime] = None,
    finished_at: Optional[Union[str, datetime.datetime]] = None,
    last_synced_at: Optional[datetime.datetime] = None,
    is_synced: Optional[Union[str, bool]] = None,
    delta_criteria: Optional[Dict[str, str]] = None,
    with_commit: bool = True,
) -> Optional[CognitionIntegration]:
    integration: CognitionIntegration = get_by_id(id)
    if not integration:
        return None

    if project_id is not None and integration.project_id is None:
        integration.project_id = project_id
    if updated_by is not None:
        integration.updated_by = updated_by
    if name is not None:
        integration.name = name
    if description is not None:
        integration.description = description
    if tokenizer is not None:
        integration.tokenizer = tokenizer
    if state is not None:
        integration.state = state.value
    if integration_config is not None:
        integration.config = integration_config
        flag_modified(integration, "config")
    if llm_config is not None:
        integration.llm_config = llm_config
        flag_modified(integration, "llm_config")
    if started_at is not None:
        integration.started_at = started_at
    if last_synced_at is not None:
        integration.last_synced_at = last_synced_at
    if delta_criteria is not None:
        integration.delta_criteria = delta_criteria
        flag_modified(integration, "delta_criteria")
    if error_message is not None:
        if error_message == "NULL":
            integration.error_message = None
        else:
            integration.error_message = error_message
    if is_synced is not None:
        if is_synced == "NULL":
            integration.is_synced = None
        else:
            integration.is_synced = is_synced
    if finished_at is not None:
        if finished_at == "NULL":
            integration.finished_at = None
        else:
            integration.finished_at = finished_at

    general.add(integration, with_commit)
    return integration


def execution_finished(id: str) -> bool:
    if not get_by_id(id):
        return True
    return bool(
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.id == id,
            CognitionIntegration.state.in_(FINISHED_STATES),
        )
        .first()
    )


def delete_many(
    ids: List[str], delete_cognition_groups: bool = True, with_commit: bool = True
) -> None:
    for id in ids:
        integration_records, IntegrationModel = (
            integration_records_bo.get_all_by_integration_id(id)
        )
        integration_records_bo.delete_many(
            IntegrationModel,
            ids=[rec.id for rec in integration_records],
            with_commit=True,
        )

    (
        session.query(CognitionIntegration)
        .filter(CognitionIntegration.id.in_(ids))
        .delete(synchronize_session=False)
    )
    if delete_cognition_groups:
        (
            session.query(CognitionGroup)
            .filter(CognitionGroup.meta_data.op("->>")("integration_id").in_(ids))
            .delete(synchronize_session=False)
        )

    general.flush_or_commit(with_commit)


def get_sharepoint_permissions_by_integration_id(
    integration_id: str,
) -> Dict[str, Any]:
    integration_id = prevent_sql_injection(
        integration_id, isinstance(integration_id, str)
    )
    query = f"""SELECT permission_id, object_id
    FROM (
    SELECT json_array_elements_text(permissions) permission_id, MAX(id::TEXT)::UUID id
    FROM integration.sharepoint
    WHERE integration_id = '{integration_id}'
    GROUP BY 1 
    )x
    INNER JOIN integration.sharepoint s
        ON x.id = s.id
    """
    return session.execute(query).all()


def get_distinct_item_ids_for_all_permissions(
    integration_id: str,
) -> List[str]:
    integration_id = prevent_sql_injection(
        integration_id, isinstance(integration_id, str)
    )
    query = f"""SELECT DISTINCT x.object_id
    FROM (
        SELECT json_array_elements_text(permissions) permission_id, MAX(object_id::TEXT) object_id
        FROM integration.sharepoint
        WHERE integration_id = '{integration_id}'
        GROUP BY 1
    ) x;"""
    results = session.execute(query).all()
    if not results:
        return []

    return [row[0] for row in results if row and row[0]]


def get_last_integrations_tasks() -> List[Dict[str, Any]]:
    query = f"""
    WITH embedding_agg AS (
        SELECT
            project_id,
            jsonb_object_agg(
                state,
                jsonb_build_object(
                    'count', count,
                    'embeddings', embeddings
                )
            ) AS embeddings_by_state
        FROM (
            SELECT
                e.project_id,
                e.state,
                COUNT(*) AS count,
                jsonb_agg(
                    jsonb_build_object(
                        'createdBy', e.created_by,
                        'finishedAt', e.finished_at,
                        'id', e.id,
                        'name', e.name,
                        'startedAt', e.started_at,
                        'state', e.state
                    ) ORDER BY e.started_at DESC
                ) AS embeddings
            FROM embedding e
            GROUP BY e.project_id, e.state
        ) AS x
        GROUP BY project_id
    ),

    attribute_agg AS (
        SELECT
            project_id,
            jsonb_object_agg(
                state,
                jsonb_build_object(
                    'count', count,
                    'attributes', attributes
                )
            ) AS attributes_by_state
        FROM (
            SELECT
                a.project_id,
                a.state,
                COUNT(*) AS count,
                jsonb_agg(
                    jsonb_build_object(
                        'dataType', a.data_type,
                        'finishedAt', a.finished_at,
                        'id', a.id,
                        'name', a.name,
                        'startedAt', a.started_at,
                        'state', a.state
                    ) ORDER BY a.started_at DESC
                ) AS attributes
            FROM attribute a
            WHERE a.state NOT IN ('UPLOADED','AUTOMATICALLY_CREATED')
            GROUP BY a.project_id, a.state
        ) AS x
        GROUP BY project_id
    ),

    record_tokenization_task_agg AS (
        SELECT
            project_id,
            jsonb_object_agg(
                state,
                jsonb_build_object(
                    'count', count,
                    'record_tokenization_tasks', record_tokenization_tasks
                )
            ) AS record_tokenization_tasks_by_state
        FROM (
            SELECT
                rtt.project_id,
                rtt.state,
                COUNT(*) AS count,
                jsonb_agg(
                    jsonb_build_object(
                        'finishedAt', rtt.finished_at,
                        'id', rtt.id,
                        'startedAt', rtt.started_at,
                        'state', rtt.state,
                        'type', rtt.type
                    ) ORDER BY rtt.started_at DESC
                ) AS record_tokenization_tasks
            FROM record_tokenization_task rtt
            GROUP BY rtt.project_id, rtt.state
        ) AS x
        GROUP BY project_id
    ),

    integration_data AS (
        SELECT 
            i.id AS integration_id,
            i.name AS integration_name,
            i.error_message,
            i.started_at,
            i.finished_at,
            i.state,
            i.organization_id,
            i.project_id,
            i.created_by,
            i.type,
            o.name AS organization_name,
            p.name AS project_name,
            jsonb_build_object(
                'embeddingsByState', coalesce(ea.embeddings_by_state, '[]'::jsonb),
                'attributesByState', coalesce(aa.attributes_by_state, '[]'::jsonb),
                'recordTokenizationTasksByState', coalesce(rtt.record_tokenization_tasks_by_state, '[]'::jsonb)
            ) AS full_data
        FROM cognition.integration i
        LEFT JOIN embedding_agg ea 
        ON ea.project_id = i.project_id
        LEFT JOIN attribute_agg aa 
        ON aa.project_id = i.project_id
        LEFT JOIN record_tokenization_task_agg rtt 
        ON rtt.project_id = i.project_id
        JOIN organization o
        ON o.id = i.organization_id
        JOIN project p
        ON p.id = i.project_id
    )

    SELECT 
        int_data.organization_id as organization_id,
        int_data.organization_name as organization_name,
        int_data.integration_id,
        int_data.integration_name,
        int_data.error_message,
        int_data.started_at,
        int_data.finished_at,
        int_data.state,
        int_data.full_data,
        int_data.created_by,
        int_data.type,
        int_data.project_name
    FROM integration_data int_data
    ORDER BY int_data.organization_id, int_data.started_at DESC
    """

    return general.execute_all(query)


def get_integration_etl_all_finished(integration_id: str) -> bool:
    IntegrationModel = integration_records_bo.integration_model(integration_id)

    etl_tasks_finished = (
        session.query(EtlTask)
        .join(
            IntegrationModel,
            (EtlTask.id == IntegrationModel.etl_task_id)
            & (IntegrationModel.integration_id == integration_id),
        )
        .filter(EtlTask.is_active)
        .filter(EtlTask.state.notin_(FINISHED_STATES))
        .first()
    ) is None

    if not etl_tasks_finished:
        return False
    integration = get_by_id(integration_id)
    return integration.state in FINISHED_STATES
