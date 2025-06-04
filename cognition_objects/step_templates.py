from typing import List, Dict, Any

# from sqlalchemy.orm.attributes import flag_modified
from ..enums import StrategyStepType
from ..business_objects import general
from ..session import session
from ..models import StepTemplates
from ..util import prevent_sql_injection, sql_alchemy_to_dict


def get(organization_id: str, template_id: str) -> StepTemplates:
    return (
        session.query(StepTemplates)
        .filter(
            StepTemplates.organization_id == organization_id,
            StepTemplates.id == template_id,
        )
        .first()
    )


def get_all_by_org_id(organization_id: str) -> List[Dict[str, Any]]:
    values = [
        sql_alchemy_to_dict(st)
        for st in (
            session.query(StepTemplates)
            .filter(
                StepTemplates.organization_id == organization_id,
            )
            .order_by(StepTemplates.created_at.asc())
            .all()
        )
    ]

    query = f"""
    SELECT jsonb_object_agg(id,C)
    FROM (
        SELECT ss.config->>'templateId' id, COUNT(*)c
        FROM cognition.strategy_step ss
        INNER JOIN cognition.project p
            ON ss.project_id = p.id
        WHERE p.organization_id = '{organization_id}'
        AND ss.step_type = '{StrategyStepType.TEMPLATED.value}'
        GROUP BY 1 
    )X
    """
    template_counts = general.execute_first(query)
    template_counts = (
        template_counts[0] if template_counts and template_counts[0] else {}
    )

    values = [
        {**s, "usage_count": template_counts.get(str(s["id"]), 0)} for s in values
    ]

    return values


def get_all_by_user(organization_id: str, user_id: str) -> List[StepTemplates]:
    return (
        session.query(StepTemplates)
        .filter(
            StepTemplates.organization_id == organization_id,
            StepTemplates.created_by == user_id,
        )
        .order_by(StepTemplates.created_at.asc())
        .all()
    )


# result structure:
# {<project_id>: {
#     "<strategy_id>": {
#         "strategy_name": <name>,
#         "order": <order>,
#         "steps": [
#             {
#                 "step_name": <name>,
#                 "step_description": <description>,
#                 "step_type": <type>,
#                 "progress_text": <progress_text>,
#                 "execute_if_source_code": <execute_if_source_code>,
#                 "config": <config>,
#                 "position": <position>
#             },
#             ...
#         ]
#     },
#     ...
# }}
def get_all_existing_steps_for_template_creation(org_id: str) -> Dict[str, Any]:
    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))
    query = f"""
    SELECT
    jsonb_object_agg(proj.project_id::text, proj.proj_json) AS all_projects
    FROM (
    SELECT
        p.id AS project_id,
        jsonb_build_object(
        'project_name', p.name,
        'strategies',
            jsonb_object_agg(
            s.id::text,
            jsonb_build_object(
                'strategy_name', s.name,
                'order',          s."order",
                'steps',
                coalesce(
                    (
                    SELECT jsonb_agg(
                            jsonb_build_object(
                                'step_id',                 ss.id,
                                'step_name',               ss.name,
                                'step_description',        ss.description,
                                'step_type',               ss.step_type,
                                'progress_text',           ss.progress_text,
                                'execute_if_source_code',  ss.execute_if_source_code,
                                'config',                  ss.config,
                                'position',                ss.position
                            )
                            ORDER BY ss.position
                            )
                    FROM cognition.strategy_step ss
                    WHERE ss.project_id  = p.id
                        AND ss.strategy_id = s.id
                        AND ss.step_type != '{StrategyStepType.TEMPLATED.value}'
                    ),
                    '[]'
                )
            )
            )
        ) AS proj_json
    FROM cognition.project p
    INNER JOIN cognition.strategy s
        ON s.project_id = p.id
    WHERE p.organization_id = '{org_id}' 
    GROUP BY p.id, p.name
    ) AS proj;
    """
    result = general.execute_first(query)
    if result and result[0]:
        return result[0]
    return {}


def create(
    org_id: str,
    user_id: str,
    name: str,
    description: str,
    config: Dict[str, Any],
    with_commit: bool = True,
) -> StepTemplates:
    template: StepTemplates = StepTemplates(
        organization_id=org_id,
        name=name,
        description=description,
        created_by=user_id,
        config=config,
    )
    general.add(template, with_commit)

    return template


# def update(
#     project_id: str,
#     strategy_step_id: str,
#     name: Optional[str] = None,
#     description: Optional[str] = None,
#     position: Optional[int] = None,
#     config: Optional[Dict] = None,
#     progress_text: Optional[str] = None,
#     execute_if_source_code: Optional[str] = None,
#     with_commit: bool = True,
# ) -> CognitionStrategyStep:
#     strategy_step: CognitionStrategyStep = get(project_id, strategy_step_id)

#     if name is not None:
#         strategy_step.name = name
#     if description is not None:
#         strategy_step.description = description
#     if position is not None:
#         strategy_step.position = position
#     if config is not None:
#         if not strategy_step.config:
#             strategy_step.config = {}
#         for key in config:
#             strategy_step.config[key] = config[key]
#         flag_modified(strategy_step, "config")
#     if progress_text is not None:
#         strategy_step.progress_text = progress_text
#     if execute_if_source_code is not None:
#         strategy_step.execute_if_source_code = execute_if_source_code

#     general.flush_or_commit(with_commit)
#     return strategy_step


# def update_or_insert_config_value(
#     project_id: str,
#     strategy_step_id: str,
#     configKey: str,
#     configValue: Any,  # also None!
#     with_commit: bool = True,
# ) -> CognitionStrategyStep:
#     strategy_step: CognitionStrategyStep = get(project_id, strategy_step_id)
#     if not strategy_step:
#         raise ValueError(f"Strategy step with id {strategy_step_id} not found")

#     if not strategy_step.config:
#         strategy_step.config = {}
#     strategy_step.config[configKey] = configValue
#     flag_modified(strategy_step, "config")

#     general.flush_or_commit(with_commit)
#     return strategy_step


def delete(org_id: str, template_id: str, with_commit: bool = True) -> None:
    session.query(StepTemplates).filter(
        StepTemplates.organization_id == org_id,
        StepTemplates.id == template_id,
    ).delete()
    general.flush_or_commit(with_commit)
