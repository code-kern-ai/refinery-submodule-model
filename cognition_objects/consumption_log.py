from ..models import CognitionConsumptionLog
from ..business_objects import general
from .. import enums
from ..util import prevent_sql_injection
from typing import Optional


def create(
    organization_id: str,
    project_id: str,
    strategy_id: str,
    conversation_id: str,
    message_id: str,
    created_by: str,
    complexity: enums.StrategyComplexity,
    state: enums.ConsumptionLogState,
    project_name: str,
    project_state: enums.CognitionProjectState,
    with_commit: bool = True,
) -> CognitionConsumptionLog:
    consumption_log = CognitionConsumptionLog(
        organization_id=organization_id,
        project_id=project_id,
        strategy_id=strategy_id,
        conversation_id=conversation_id,
        message_id=message_id,
        created_by=created_by,
        complexity=complexity.value,
        state=state.value,
        project_name=project_name,
        project_state=project_state.value,
    )
    general.add(consumption_log, with_commit=with_commit)
    return consumption_log


def update_project_name(
    project_id: str, project_name: str, with_commit: bool = True
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    project_name = prevent_sql_injection(project_name, isinstance(project_name, str))
    query = f"""
    UPDATE cognition.consumption_log
    SET project_name = '{project_name}'
    WHERE project_id = '{project_id}'
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def get_details(
    organization_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    order_desc: bool = True,
    as_query: bool = False,
) -> list:
    where_add = ""

    organization_id = prevent_sql_injection(
        organization_id, isinstance(organization_id, str)
    )

    if start_date and end_date:
        start_date = prevent_sql_injection(start_date, isinstance(start_date, str))
        end_date = prevent_sql_injection(end_date, isinstance(end_date, str))
        where_add += f"AND cl.created_at BETWEEN '{start_date}' AND '{end_date}'"

    order_key = "DESC" if order_desc else "ASC"

    query = f"""
    SELECT COALESCE(p.name, cl.project_name) AS project, COALESCE(s.name, 'deleted') AS strategy, cl.created_at, cl.complexity, cl.state, cl.project_state
    FROM cognition.consumption_log cl
    LEFT JOIN cognition.project p
        ON p.id = cl.project_id
    LEFT JOIN cognition.strategy s
        ON s.id = cl.strategy_id
    WHERE cl.organization_id = '{organization_id}' {where_add}
    ORDER BY cl.created_at {order_key}
    """
    if as_query:
        return query
    return general.execute_all(query)
