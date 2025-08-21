from typing import Any, Dict, List, Union
from datetime import datetime, timedelta

from ..business_objects import general
from ..session import session
from ..models import SumsTable
from ..enums import LLMProvider, StrategyStepType


def get(id: str) -> SumsTable:
    return (
        session.query(SumsTable)
        .filter(
            SumsTable.id == id,
        )
        .first()
    )


def get_all_by_key(sum_key: str) -> List[SumsTable]:
    return (
        session.query(SumsTable)
        .filter(
            SumsTable.sum_key == sum_key,
        )
        .all()
    )


def get_last_execution_by_key(sum_key: str) -> datetime:
    entry = (
        session.query(SumsTable)
        .filter(
            SumsTable.sum_key == sum_key,
        )
        .order_by(SumsTable.created_at.desc())
        .first()
    )
    if entry:
        return entry.created_at
    return None


def get_privatemode_sum_snapshot(as_query: bool = False) -> Dict[str, Any]:
    ## counts messages created with something related to privatemode
    ## this means either tmp_doc, llm or templated values
    ## the messages are count distinct for the previous day to understand when and how much it's used
    ## deleted messages on the fly are not included, for this we would need to track them on creation
    query = f"""
    SELECT json_build_object('counted_for',(CURRENT_DATE - INTERVAL '1 day')::date,'values',array_agg(row_to_json(y)))
    FROM (
        SELECT  o.id organization_id, o.name organization_name, p.id project_id, p.name project_name, is_kern_user, COUNT(*) 
        FROM (
            SELECT pl.project_id, pl.message_id, CASE WHEN u.email LIKE '%@kern.ai' THEN TRUE ELSE FALSE END is_kern_user
            FROM cognition.pipeline_logs pl
            INNER JOIN cognition.strategy_step ss
                ON pl.project_id = ss.project_id AND pl.strategy_step_id = ss.id
            INNER JOIN PUBLIC.user u
                ON pl.created_by = u.id
            WHERE pl.created_at::date = CURRENT_DATE - INTERVAL '1 day'
            AND ss.step_type IN ('{StrategyStepType.LLM.value}' ,'{StrategyStepType.QUERY_REPHRASING.value}','{StrategyStepType.HEADER.value}')
              AND ss.config->>'llmIdentifier' = '{LLMProvider.PRIVATEMODE_AI.value}'
            UNION
            SELECT pl.project_id, pl.message_id, CASE WHEN u.email LIKE '%@kern.ai' THEN TRUE ELSE FALSE END is_kern_user
            FROM cognition.pipeline_logs pl
            INNER JOIN cognition.strategy_step ss
                ON pl.project_id = ss.project_id AND pl.strategy_step_id = ss.id AND ss.step_type = '{StrategyStepType.TEMPLATED.value}'
            INNER JOIN cognition.step_templates st
                ON (ss.config->>'templateId')::UUID = st.id
            INNER JOIN PUBLIC.user u
                ON pl.created_by = u.id
            WHERE pl.created_at::date = CURRENT_DATE - INTERVAL '1 day'
            AND pl.strategy_step_type = '{StrategyStepType.TEMPLATED.value}'
            AND st.config::jsonb -> 'steps' @> '[{{"config": {{"llmIdentifier": "{LLMProvider.PRIVATEMODE_AI.value}"}}}}]'
            UNION 
            SELECT pl.project_id, pl.message_id, CASE WHEN u.email LIKE '%@kern.ai' THEN TRUE ELSE FALSE END is_kern_user
            FROM cognition.pipeline_logs pl
            INNER JOIN cognition.project p
                ON pl.project_id = p.id
            INNER JOIN PUBLIC.user u
                ON pl.created_by = u.id
            WHERE pl.created_at::date = CURRENT_DATE - INTERVAL '1 day'
            AND pl.strategy_step_type = '{StrategyStepType.TMP_DOC_RETRIEVAL.value}'
            AND (p.llm_config::jsonb -> 'extraction' ->> 'llmIdentifier' = 'PRIVATEMODE_AI' --written like the enum here so not interpolated
            OR p.llm_config::jsonb -> 'transformation' ->> 'llmIdentifier' = 'PRIVATEMODE_AI')  --written like the enum here so not interpolated
        ) x
        INNER JOIN cognition.project p
            ON x.project_id = p.id
        INNER JOIN organization o
            ON p.organization_id = o.id
        group BY o.id, p.id,is_kern_user
    ) y """
    if as_query:
        return query
    result = general.execute_first(query)
    if result and result[0]:
        return result[0]
    return None


def create(
    sum_key: str,
    data: Union[List[Any], Dict[str, Any]],
    with_commit: bool = True,
) -> SumsTable:
    obj = SumsTable(
        sum_key=sum_key,
        data=data,
    )
    general.add(obj, with_commit)

    return obj


def clean_old_entries(sum_key: str, delta: timedelta, with_commit: bool = True) -> None:

    session.query(SumsTable).filter(
        SumsTable.sum_key == sum_key,
        SumsTable.created_at < datetime.now() - delta,
    ).delete()
    general.flush_or_commit(with_commit)
