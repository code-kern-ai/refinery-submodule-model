import uuid
from datetime import date
from ..enums import StrategyComplexity
from . import project
from ..business_objects import general
from typing import Optional
from ..util import prevent_sql_injection


def log_consumption(
    org_id: str,
    project_id: str,
    complexity: StrategyComplexity,
    with_commit: bool = True,
):
    id = str(uuid.uuid4())
    project_entity = project.get(project_id)
    project_name = project_entity.name

    query = f"""
    INSERT INTO cognition.consumption_summary (id, organization_id, project_id, creation_date, project_name, complexity, count)
    VALUES ('{id}', '{org_id}', '{project_id}', '{date.today()}', '{project_name}', '{complexity.value}', 1)
    ON CONFLICT ON CONSTRAINT unique_summary DO UPDATE
        SET count = consumption_summary.count + 1;
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def get_consumption_summary(
    org_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    as_query: bool = False,
):
    where_add = ""
    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))

    if start_date and end_date:
        start_date = prevent_sql_injection(start_date, isinstance(start_date, str))
        end_date = prevent_sql_injection(end_date, isinstance(end_date, str))
        where_add += f"AND creation_date BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
    SELECT date_part('year', creation_date) AS year, date_part('month', creation_date) AS month, complexity, SUM(count) as count
    FROM cognition.consumption_summary
    WHERE organization_id = '{org_id}' {where_add}
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """
    if as_query:
        return query
    return general.execute(query)
