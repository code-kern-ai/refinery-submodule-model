from typing import List, Dict, Any, Iterable, Optional

from sqlalchemy.orm.attributes import flag_modified
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
        'created_at', p.created_at,
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
                                'src_step_id',             ss.id,
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


def get_step_template_progress_text_lookup_for_strategy(
    project_id: str, strategy_id: str
) -> Dict[str, str]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    strategy_id = prevent_sql_injection(strategy_id, isinstance(strategy_id, str))
    query = f"""
   WITH base AS (
    SELECT
        st.config                            AS config,
        ss.config->'variableValues'          AS variableValues,
        ss.id step_id
    FROM cognition.strategy_step ss
    INNER JOIN cognition.project p
        ON ss.project_id = p.id
    INNER JOIN cognition.step_templates st
        ON st.id = (ss.config->>'templateId')::UUID
    AND st.organization_id = p.organization_id
    WHERE
        ss.project_id  = '{project_id}'
        AND ss.strategy_id = '{strategy_id}'
        AND ss.step_type   = '{StrategyStepType.TEMPLATED.value}'
    )


    SELECT jsonb_object_agg(step_id,progress_lookup)
    FROM (
        SELECT 
            step_id,
            array_agg(jsonb_build_object('template_key',dict_key,'progress_text',resolved_progress_text)) progress_lookup
        FROM (
            SELECT
            -- Extract one step-object at a time
            step_id,
            (step_item ->> 'stepName')      AS step_name,
            step_item->>'stepType'|| '@' || (step_item->>'srcStepId')::TEXT AS dict_key,
            -- Raw progressText from the JSON
            (step_item ->> 'progressText')  AS raw_progress_text,
            -- If raw_progress_text matches @@var_<id>@@, replace via variableValues
            CASE
                WHEN (step_item ->> 'progressText') ~ '^@@var_[0-9a-fA-F\-]{{36}}@@$'
                THEN
                -- Extract the UUID between "var_" and "@@"
                (
                    SELECT variableValues ->> inner_uuid
                    FROM (
                    SELECT
                        regexp_replace(step_item ->> 'progressText',
                                    '^@@var_([0-9a-fA-F\-]{{36}})@@$',
                                    '\\1') AS inner_uuid
                    ) AS sub
                )
                ELSE
                (step_item ->> 'progressText')
            END AS resolved_progress_text
            FROM base
            -- Unnest the steps array
            CROSS JOIN LATERAL json_array_elements(base.config->'steps') AS step_item 
        )x
        GROUP BY 1
    )y
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


def update(
    org_id: str,
    template_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    with_commit: bool = True,
) -> StepTemplates:
    template = get(org_id, template_id)
    if not template:
        raise ValueError(
            f"Template with ID {template_id} not found in organization {org_id}."
        )

    if name is not None:
        template.name = name
    if description is not None:
        template.description = description
    if config is not None:
        template.config = config
        flag_modified(template, "config")

    general.flush_or_commit(with_commit)

    return template


def delete(org_id: str, template_id: str, with_commit: bool = True) -> None:
    session.query(StepTemplates).filter(
        StepTemplates.organization_id == org_id,
        StepTemplates.id == template_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_many(
    org_id: str, template_ids: Iterable[str], with_commit: bool = True
) -> None:
    session.query(StepTemplates).filter(
        StepTemplates.organization_id == org_id,
        StepTemplates.id.in_(template_ids),
    ).delete()
    general.flush_or_commit(with_commit)
