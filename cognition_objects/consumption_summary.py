import uuid
from datetime import date
from ..enums import StrategyComplexity
from . import project
from ..business_objects import general


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
    INSERT INTO cognition.consumption_summary (id, organization_id, project_id, date, project_name, complexity, count)
    VALUES ('{id}', '{org_id}', '{project_id}', '{date.today()}', '{project_name}', '{complexity.value}', 1)
    ON CONFLICT (organization_id, project_id, date, complexity) DO UPDATE
        SET count = consumption_summary.count + 1;
    """
    general.execute(query)
    general.flush_or_commit(with_commit)
