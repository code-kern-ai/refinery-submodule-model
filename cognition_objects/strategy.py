from typing import Any, List, Optional
from datetime import datetime

from submodules.model.util import prevent_sql_injection

from ..business_objects import general
from ..session import session
from ..models import CognitionStrategy
from ..enums import StrategyComplexity, StrategyStepType
from ..business_objects import cross_selling as cross_selling_bo


def get(project_id: str, strategy_id: str) -> CognitionStrategy:
    return (
        session.query(CognitionStrategy)
        .filter(
            CognitionStrategy.project_id == project_id,
            CognitionStrategy.id == strategy_id,
        )
        .first()
    )


def get_by_name(project_id: str, name: str) -> CognitionStrategy:
    return (
        session.query(CognitionStrategy)
        .filter(
            CognitionStrategy.project_id == project_id, CognitionStrategy.name == name
        )
        .first()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionStrategy]:
    return (
        session.query(CognitionStrategy)
        .filter(CognitionStrategy.project_id == project_id)
        .order_by(CognitionStrategy.order.desc())
        .order_by(CognitionStrategy.created_at.desc())
        .all()
    )


def get_strategies_without_complexity() -> List[CognitionStrategy]:
    return (
        session.query(CognitionStrategy)
        .filter(CognitionStrategy.complexity == None)
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    name: str,
    description: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    order: Optional[int] = None,
    id: Optional[str] = None,
) -> CognitionStrategy:
    strategy: CognitionStrategy = CognitionStrategy(
        id=id,
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        order=order,
    )
    general.add(strategy, with_commit)

    return strategy


def update(
    project_id: str,
    strategy_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    complexity: Optional[StrategyComplexity] = None,
    order: Optional[int] = None,
    with_commit: bool = True,
) -> CognitionStrategy:
    strategy = get(project_id, strategy_id)
    if name is not None:
        strategy.name = name
    if description is not None:
        strategy.description = description
    if complexity is not None:
        strategy.complexity = complexity.value
    if order is not None:
        strategy.order = order

    general.add(strategy, with_commit)

    return strategy


def delete(project_id: str, strategy_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategy).filter(
        CognitionStrategy.project_id == project_id,
        CognitionStrategy.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_all_by_project_id(project_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategy).filter(
        CognitionStrategy.project_id == project_id
    ).delete()
    general.flush_or_commit(with_commit)


def get_strategies_info(
    step_types: List[str],
    created_at_from: str,
    created_at_to: Optional[str] = None,
    cross_selling_filter: Optional[str] = None,
) -> List[Any]:

    step_types = [prevent_sql_injection(st, isinstance(st, str)) for st in step_types]
    if len(step_types) == 0:
        return []

    created_at_from = prevent_sql_injection(
        created_at_from, isinstance(created_at_from, str)
    )
    if created_at_to:
        created_at_to = prevent_sql_injection(
            created_at_to, isinstance(created_at_to, str)
        )
    created_at_to_filter = ""
    cross_selling_filter_sql = cross_selling_bo.build_cross_selling_filter_sql(
        cross_selling_filter
    )
    if cross_selling_filter_sql:
        cross_selling_filter_sql = " AND " + cross_selling_filter_sql

    if created_at_to:
        created_at_to_filter = f"AND ss.created_at <= '{created_at_to}'"

    step_types_sql = ", ".join([f"'{st}'" for st in step_types])

    query = f"""
    WITH step_data AS (
        SELECT 
            s.id AS strategy_id, s.name AS strategy_name,
            ss.id AS step_id, ss.created_by,ss.created_at, ss.name AS step_name, ss.step_type,
            p.name AS project_name, p.id AS project_id,
            o.name AS organization_name, o.id AS organization_id,
            cs.name AS cross_selling_name,
            st.config::jsonb AS template_config,
            CASE 
                WHEN ss.step_type = '{StrategyStepType.TEMPLATED.value}' AND st.config IS NOT NULL
                THEN ARRAY(
                    SELECT (t->>'stepType') || ':' || (t->>'stepName')
                    FROM jsonb_array_elements((st.config->'steps')::jsonb) t
                )
                ELSE NULL
            END AS template_step_names,
            CASE
                WHEN ss.step_type = '{StrategyStepType.TEMPLATED.value}' AND st.config IS NOT NULL
                THEN ARRAY(
                    SELECT t->>'stepType'
                    FROM jsonb_array_elements((st.config->'steps')::jsonb) t
                )
                ELSE NULL
            END AS template_step_types
        FROM cognition.strategy s
        JOIN cognition.strategy_step ss 
        ON ss.strategy_id = s.id
        JOIN cognition.project p 
        ON p.id = s.project_id
        JOIN organization o 
        ON o.id = p.organization_id
        LEFT JOIN cross_selling cs ON cs.id = o.cross_selling_id
        LEFT JOIN cognition.step_templates st 
        ON st.id = (ss.config->>'templateId')::uuid
        WHERE ss.created_at >= '{created_at_from}'
        {created_at_to_filter}
        {cross_selling_filter_sql}
    )
    SELECT strategy_id, strategy_name, step_id, created_by, created_at, step_name, step_type, project_name, project_id, organization_name, organization_id, cross_selling_name,
        CASE
            WHEN step_type = '{StrategyStepType.TEMPLATED.value}' THEN template_step_names
            ELSE ARRAY[step_type || ':' || step_name]
        END AS templated_step_names
    FROM step_data
    WHERE 
        step_type IN ({step_types_sql})
        OR (step_type = '{StrategyStepType.TEMPLATED.value}' AND template_step_types && ARRAY[{step_types_sql}])
    ORDER BY strategy_id, created_at DESC
    """

    return general.execute_all(query)
