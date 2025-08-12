from typing import Dict, List, Any
from ..util import prevent_sql_injection

from ..business_objects import general

from sqlalchemy.engine.row import Row


from .. import enums

ENGINEERING_TEAM_INDICATOR = "ENGINEERING_TEAM"
PERIOD_OPTIONS = {"days", "weeks", "months"}


def get_result_admin_query(
    query: enums.AdminQueries,
    parameters: Dict[str, Any],
    as_query: bool = False,
) -> List[Row]:
    if parameters is None:
        parameters = {}
    if query == enums.AdminQueries.USERS_TO_PROJECTS:
        return __get_users_to_projects(**parameters, as_query=as_query)
    if query == enums.AdminQueries.USERS_BY_ORG:
        return __get_users_by_org(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.ACTIVE_USERS_GLOBAL:
        return __get_active_users_global(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.ACTIVE_USERS_BY_ORG:
        return __get_active_users_by_org(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.MESSAGES_CREATED:
        return __get_messages_created(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.MESSAGES_CREATED_BY_PROJECT:
        return __get_messages_created_by_project(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.MESSAGES_FEEDBACK_PER_PROJECT:
        return __get_messages_feedback_by_project(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.AVG_MESSAGES_PER_CONVERSATION_GLOBAL:
        return __get_global_messages_per_conversation(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.AVG_MESSAGES_PER_CONVERSATION:
        return __get_avg_messages_per_conversation(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.MACRO_EXECUTIONS:
        return __get_macro_executions(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.FOLDER_MACRO_EXECUTION_SUMMARY:
        return __get_folder_macro_execution_summary(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.CREATED_TAGS_PER_ORG:
        return __get_created_tags_per_org(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.CONVERSATIONS_PER_TAG:
        return __get_conversations_per_tag(**parameters, as_query=as_query)
    elif query == enums.AdminQueries.MULTITAGGED_CONVERSATIONS:
        return __get_multitagged_conversations(**parameters, as_query=as_query)

    return []


def __get_multitagged_conversations(
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
):

    org_join = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_join = f""" INNER JOIN cognition.project p 
            ON c.project_id = p.id AND p.organization_id = '{organization_id}'"""

    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON c.created_by = u.id AND u.email NOT LIKE '%@kern.ai'"""

    query = f"""
        SELECT o.name organization_name, p.name project_name, COUNT(*) conv_with_gr_1_tag
    FROM (
        SELECT c.project_id, conversation_id
        FROM cognition.conversation C
        {filter_join}
        INNER JOIN cognition.conversation_tag_association cta
            ON	c.id = cta.conversation_id
        {org_join}
        group BY 1, 2
        HAVING COUNT(*) > 1 
    ) x
    INNER JOIN cognition.project p
        ON x.project_id = p.id
    INNER JOIN organization o
        ON p.organization_id = o.id
    group BY 1,2
    """
    if as_query:
        return query
    return general.execute_all(query)


def __get_conversations_per_tag(
    organization_id: str = "",
    without_kern_email: bool = False,
    distinct_conversations: bool = False,
    as_query: bool = False,
):

    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" WHERE o.id = '{organization_id}'"""

    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON c.created_by = u.id AND u.email NOT LIKE '%@kern.ai'"""

    count_query = "*"
    if distinct_conversations:
        count_query = "DISTINCT c.id"

    query = f"""
    SELECT o.name organization_name, p.name project_name, COUNT({count_query}) tags_created
    FROM cognition.conversation_tag_association cta
    INNER JOIN cognition.conversation c
        ON c.id = cta.conversation_id
    {filter_join}
    INNER JOIN cognition.project p
        ON c.project_id = p.id
    INNER JOIN organization o
        ON p.organization_id = o.id
    {org_where}
    GROUP BY 1,2
    """

    if as_query:
        return query
    return general.execute_all(query)


def __get_created_tags_per_org(
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
):

    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" WHERE o.id = '{organization_id}'"""

    filter_join = ""
    if without_kern_email:
        filter_join = """AND u.email NOT LIKE '%@kern.ai'"""

    query = f"""
    SELECT 
        o.name organization_name, COUNT(*)
    FROM organization o
    INNER JOIN PUBLIC.user u
        ON o.id = u.organization_id {filter_join}
    INNER JOIN cognition.conversation_tag ct
        ON u.id = ct.created_by
    {org_where}
    GROUP BY 1
    ORDER BY 1
    """

    if as_query:
        return query
    return general.execute_all(query)


def __get_folder_macro_execution_summary(
    slices: int = 7,  # how many chunks are relevant
    organization_id: str = "",
    as_query: bool = False,
) -> List[Row]:

    slices = max(min(slices, 30), 1)

    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" WHERE me.organization_id = '{organization_id}'"""

    query = f"""
    WITH params AS (
        SELECT
        'months'   ::text AS period, -- sum table so fixed to months
        {slices}         ::int  AS n     -- ← how many of those periods you want
    ),

    -- 1) build the list of period-start dates
    periods AS (
        SELECT
        (generate_series(
            date_trunc(p.period, CURRENT_DATE)
            - (p.n - 1) * ( '1 ' || p.period )::interval,
            date_trunc(p.period, CURRENT_DATE),
            ( '1 ' || p.period )::interval
        ))::date AS period_start,
        p.period
        FROM params p
    ),
    filtered AS (
        SELECT
            me.organization_id,
            date_trunc(p.period, me.creation_month)::date AS period_start,
            execution_count,
            processed_files_count
        FROM cognition.macro_execution_summary me
        INNER JOIN params p
            ON me.creation_month >= (
                SELECT MIN(period_start)
                FROM periods
                )
            AND me.creation_month < (
                SELECT MAX(period_start) + ( '1 ' || p.period )::interval
                FROM periods, params
                )
        {org_where}
    )


    SELECT 
        o.name organization_name,
        period_start,
        (period_start + ( '1 ' || pa.period  )::INTERVAL  - '1 day'::interval  )::date AS period_end,
        execution_count,
        processed_files_count
    FROM filtered m
    INNER JOIN organization o
        ON m.organization_id = o.id
    , params pa
    ORDER BY 1,2 DESC
"""
    if as_query:
        return query
    return general.execute_all(query)


def __get_macro_executions(
    period: str = "days",  # options: days, weeks, months
    slices: int = 7,  # how many chunks are relevant
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:

    if period not in PERIOD_OPTIONS:
        raise ValueError(f"Invalid period: {period}. Must be one of {PERIOD_OPTIONS}.")
    slices = max(min(slices, 30), 1)

    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" WHERE me.organization_id = '{organization_id}'"""

    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON me.created_by = u.id AND u.email NOT LIKE '%@kern.ai'
        """

    query = f"""
    WITH params AS (
        SELECT
        '{period}'   ::text AS period,   -- ← 'days' | 'weeks' | 'months'
        {slices}         ::int  AS n     -- ← how many of those periods you want
    ),

    -- 1) build the list of period-start dates
    periods AS (
        SELECT
        (generate_series(
            date_trunc(p.period, CURRENT_DATE)
            - (p.n - 1) * ( '1 ' || p.period )::interval,
            date_trunc(p.period, CURRENT_DATE),
            ( '1 ' || p.period )::interval
        ))::date AS period_start,
        p.period
        FROM params p
    ),
    filtered AS (
        SELECT
            me.organization_id,
            date_trunc(p.period, me.created_at)::date AS period_start
        FROM cognition.macro_execution me
        {filter_join}
        INNER JOIN params p
            ON me.created_at >= (
                SELECT MIN(period_start)
                FROM periods
                )
            AND me.created_at < (
                SELECT MAX(period_start) + ( '1 ' || p.period )::interval
                FROM periods, params
                )
        {org_where}
    )


    SELECT 
        o.name organization_name,
        period_start,
        (period_start + ( '1 ' || pa.period  )::INTERVAL  - '1 day'::interval  )::date AS period_end,
        macro_executions
    FROM (
        SELECT 
                organization_id,
                period_start,
                COUNT(*) macro_executions
            FROM filtered M
        GROUP BY 
            m.organization_id,
            period_start
    )y
    INNER JOIN organization o
        ON y.organization_id = o.id
    , params pa
    ORDER BY 1,2 DESC
"""
    if as_query:
        return query
    return general.execute_all(query)


def __get_avg_messages_per_conversation(
    period: str = "days",  # options: days, weeks, months
    slices: int = 7,  # how many chunks are relevant
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:

    if period not in PERIOD_OPTIONS:
        raise ValueError(f"Invalid period: {period}. Must be one of {PERIOD_OPTIONS}.")
    slices = max(min(slices, 30), 1)

    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" INNER JOIN cognition.project pr
            ON m.project_id = pr.id AND pr.organization_id = '{organization_id}'"""
    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON m.created_by = u.id AND u.email NOT LIKE '%@kern.ai'
        """
    query = f"""
    WITH params AS (
        SELECT
        '{period}'   ::text AS period,   -- ← 'days' | 'weeks' | 'months'
        {slices}         ::int  AS n         -- ← how many of those periods you want
    ),

    -- 1) build the list of period-start dates
    periods AS (
        SELECT
        (generate_series(
            date_trunc(p.period, CURRENT_DATE)
            - (p.n - 1) * ( '1 ' || p.period )::interval,
            date_trunc(p.period, CURRENT_DATE),
            ( '1 ' || p.period )::interval
        ))::date AS period_start,
        p.period
        FROM params p
    ),
    filtered AS (
        SELECT
            m.project_id,
            date_trunc(p.period, c.created_at)::date AS period_start,
            m.conversation_id
        FROM cognition.message m
        {filter_join}
        {org_where}
        INNER JOIN cognition.conversation c
            ON m.conversation_id = c.id
        INNER JOIN params p
        ON c.created_at >= (
            SELECT MIN(period_start)
            FROM periods
            )
        AND c.created_at < (
            SELECT MAX(period_start) + ( '1 ' || p.period )::interval
            FROM periods, params
            )
    )


    SELECT 
        o.name organization_name,
        p.name project_name,
        period_start,
        (period_start + ( '1 ' || pa.period  )::INTERVAL  - '1 day'::interval  )::date AS period_end,
        cnt_conversations,
        sum_messages,
        avg_messages_per_conv
    FROM (
        SELECT 
            period_start,
            project_id,
            COUNT(*) cnt_conversations,
            SUM(cnt) sum_messages,
            ROUND(AVG(cnt),2) avg_messages_per_conv
        FROM (
            SELECT 
                project_id,
                period_start,
                conversation_id,
                COUNT(*) cnt
            FROM filtered M
        GROUP BY 
            m.project_id,
            period_start,
            m.conversation_id
        )x
        GROUP BY project_id, period_start
    )y
    INNER JOIN cognition.project p
        ON y.project_id = p.id
    INNER JOIN organization o
        ON p.organization_id = o.id
    , params pa
    ORDER BY 1,3 DESC
"""
    if as_query:
        return query
    return general.execute_all(query)


def __get_global_messages_per_conversation(
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
):
    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" INNER JOIN cognition.project pr
            ON m.project_id = pr.id AND pr.organization_id = '{organization_id}'"""

    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON m.created_by = u.id AND u.email NOT LIKE '%@kern.ai'
        """

    query = f"""
    SELECT 
        o.name organization_name,
        p.name project_name,
        cnt_conversations,
        sum_messages,
        avg_messages_per_conv
    FROM (
        SELECT 
            project_id,
            COUNT(*) cnt_conversations,
            SUM(cnt) sum_messages,
            ROUND(AVG(cnt),2) avg_messages_per_conv
        FROM (
            SELECT 
                project_id,
                conversation_id,
                COUNT(*) cnt
            FROM cognition.message M
            {filter_join}
            {org_where}
        GROUP BY 
            m.project_id,
            m.conversation_id 
        )x
        GROUP BY project_id
    )y
    INNER JOIN cognition.project p
        ON y.project_id = p.id
    INNER JOIN organization o
        ON p.organization_id = o.id
    ORDER BY 1,5 DESC """
    if as_query:
        return query
    return general.execute_all(query)


def __get_messages_feedback_by_project(
    period: str = "days",  # options: days, weeks, months
    slices: int = 7,  # how many chunks are relevant
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:

    if period not in PERIOD_OPTIONS:
        raise ValueError(f"Invalid period: {period}. Must be one of {PERIOD_OPTIONS}.")
    slices = max(min(slices, 30), 1)

    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" INNER JOIN cognition.project pr
            ON m.project_id = pr.id AND pr.organization_id = '{organization_id}'"""

    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON m.created_by = u.id AND u.email NOT LIKE '%@kern.ai'
        """
    query = f"""
    WITH params AS (
        SELECT
        '{period}'   ::text AS period,   -- ← 'days' | 'weeks' | 'months'
        {slices}         ::int  AS n         -- ← how many of those periods you want
    ),

    -- 1) build the list of period-start dates
    periods AS (
        SELECT
        (generate_series(
            date_trunc(p.period, CURRENT_DATE)
            - (p.n - 1) * ( '1 ' || p.period )::interval,
            date_trunc(p.period, CURRENT_DATE),
            ( '1 ' || p.period )::interval
        ))::date AS period_start,
        p.period
        FROM params p
    ),

    -- 2) pull in feedback-bearing messages across all projects & full span
    filtered AS (
        SELECT
            m.project_id,
            date_trunc(p.period, m.created_at)::date AS period_start,
            m.feedback_category,
            m.feedback_value
        FROM cognition.message m
        {filter_join}
        {org_where}
        INNER JOIN params p
            ON m.created_at >= (SELECT MIN(period_start) FROM periods )
            AND m.created_at < (
                SELECT MAX(period_start) + ( '1 ' || p.period )::interval
                FROM periods, params
                )
        WHERE  m.feedback_value IS NOT NULL
    ),

    agg AS (
        SELECT
            f.project_id,
            f.period_start,
            f.feedback_category,
            f.feedback_value,
        COUNT(*) AS cnt
        FROM filtered f
        GROUP BY 1,2,3,4
    ),

    -- 4) total feedback count per project & period
    totals AS (
        SELECT
        project_id,
        period_start,
        SUM(cnt) AS total_cnt
        FROM agg
        GROUP BY 1,2
    ),

    -- 5) all combinations of project and periods
    project_periods AS (
        SELECT DISTINCT
        f.project_id,
        p.period_start,
        p.period
        FROM filtered f
        CROSS JOIN periods p
    )

    SELECT 
        o.name organization_name,
        p.name project_name,
        period_start,
        period_end,
        feedback_value,
        feedback_category,
        cnt,
        pct_of_period
    FROM (
        SELECT
        pp.period_start,
        (pp.period_start
            + ( '1 ' || pp.period )::interval
            - '1 day'::interval
        )::date AS period_end,
        
        pp.project_id,
        COALESCE(a.feedback_value,    '—')  AS feedback_value,
        COALESCE(a.feedback_category, '—')  AS feedback_category,
        COALESCE(a.cnt,                0)   AS cnt,
        ROUND(
            100.0 * COALESCE(a.cnt,0)
            / NULLIF(t.total_cnt,0)
        ,2)                                AS pct_of_period
        
        FROM project_periods pp
        LEFT JOIN agg    a  ON a.period_start = pp.period_start
                        AND a.project_id  = pp.project_id
        LEFT JOIN totals t  ON t.period_start = pp.period_start
                        AND t.project_id  = pp.project_id
        
        WHERE pp.project_id IS NOT NULL AND a.cnt IS NOT NULL
    ) x
    INNER JOIN cognition.project p
        ON x.project_id = p.id
    INNER JOIN organization o
        ON p.organization_id = o.id
    ORDER BY
        period_start DESC,
        o.name,
        p.name,
        feedback_value,
        feedback_category;
"""
    if as_query:
        return query
    return general.execute_all(query)


def __get_messages_created_by_project(
    period: str = "days",  # options: days, weeks, months
    slices: int = 7,  # how many chunks are relevant
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:

    if period not in PERIOD_OPTIONS:
        raise ValueError(f"Invalid period: {period}. Must be one of {PERIOD_OPTIONS}.")
    slices = max(min(slices, 30), 1)

    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" INNER JOIN cognition.project pr
            ON m.project_id = pr.id AND pr.organization_id = '{organization_id}'"""

    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON m.created_by = u.id AND u.email NOT LIKE '%@kern.ai'
        """
    query = f"""
    WITH params AS (
        SELECT
        '{period}'   ::text AS period,   -- ← 'days' | 'weeks' | 'months'
        {slices}     ::int  AS n         -- ← how many of those periods you want
    ),
    periods AS (
        SELECT
        (generate_series(
            date_trunc(p.period, CURRENT_DATE)
            - (p.n - 1) * ( '1 ' || p.period )::interval,
            date_trunc(p.period, CURRENT_DATE),
            ( '1 ' || p.period )::interval
        ))::date    AS period_start,
        p.period
        FROM params p
    ),
    filtered_messages AS (
        SELECT
            m.project_id,
            m.created_at
        FROM cognition.message m
        {filter_join}
        {org_where}
        INNER JOIN params p
        ON  m.created_at >= (SELECT MIN(period_start) FROM periods)
        AND m.created_at <  (SELECT MAX(period_start) + ( '1 ' || p.period )::interval
                                FROM periods, params)
    ),
    aggregated AS (
        SELECT
        fm.project_id,
        date_trunc((SELECT period FROM params), fm.created_at)::date AS period_start,
        COUNT(*) AS message_count
        FROM filtered_messages fm
        GROUP BY 1,2
    )
    
    SELECT 
        o.name organization_name,
        p.name project_name,
        period_start,
        period_end,
        message_count
    FROM (
        SELECT
            pr.period_start,
            (pr.period_start + ('1 ' || pr.period)::INTERVAL - '1 days'::INTERVAL)::date period_end,
            pj.project_id,
            a.message_count AS message_count
        FROM periods pr
        CROSS JOIN (SELECT DISTINCT project_id FROM filtered_messages) pj
        LEFT JOIN aggregated a
            ON a.project_id   = pj.project_id
        AND a.period_start = pr.period_start
        WHERE message_count IS NOT NULL
    ) x
    INNER JOIN cognition.project p
        ON p.id = x.project_id
    INNER JOIN organization o
        ON p.organization_id = o.id
    ORDER BY 1,2,3 DESC
"""
    if as_query:
        return query
    return general.execute_all(query)


def __get_messages_created(
    period: str = "days",  # options: days, weeks, months
    slices: int = 7,  # how many chunks are relevant
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:

    if period not in PERIOD_OPTIONS:
        raise ValueError(f"Invalid period: {period}. Must be one of {PERIOD_OPTIONS}.")
    slices = max(min(slices, 30), 1)

    org_where = ""
    org_select = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f""" INNER JOIN cognition.project pr
            ON m.project_id = pr.id AND pr.organization_id = '{organization_id}'"""
        org_select = f"(SELECT MAX(NAME) FROM organization WHERE id = '{organization_id}') organization_name,"

    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON m.created_by = u.id AND u.email NOT LIKE '%@kern.ai'
        """
    query = f"""
    WITH
    params AS (
        SELECT
        '{period}'   ::text AS period,   -- ← 'days' | 'weeks' | 'months'
        {slices}         ::int  AS n         -- ← how many of those periods you want
    ),
    periods AS (
        SELECT
        (generate_series(
            date_trunc(p.period, CURRENT_DATE)
            - (p.n - 1) * ( '1 ' || p.period )::interval,
            date_trunc(p.period, CURRENT_DATE),
            ( '1 ' || p.period )::interval
        ))::date    AS period_start,
        p.period
        FROM params p
    ),
    filtered_messages AS (
        SELECT
            m.created_at
        FROM cognition.message m
        {filter_join}
        {org_where}
        INNER JOIN params p
            ON  m.created_at >= (SELECT MIN(period_start) FROM periods)
            AND m.created_at <  (SELECT MAX(period_start) + ( '1 ' || p.period )::interval
                                FROM periods, params)
    ),
    aggregated AS (
        SELECT
        date_trunc((SELECT period FROM params), fm.created_at)::date AS period_start,
        COUNT(*) AS message_count
        FROM filtered_messages fm
        GROUP BY 1
    )
    SELECT
        {org_select}
        pr.period_start,
        (pr.period_start + ('1 ' || pr.period)::INTERVAL - '1 days'::INTERVAL)::date period_end,
        COALESCE(a.message_count, 0) AS total_messages
    FROM periods pr
    LEFT JOIN aggregated a
        ON a.period_start = pr.period_start
    ORDER BY
        pr.period_start DESC; """
    if as_query:
        return query
    return general.execute_all(query)


def __get_active_users_by_org(
    min_msg_count: int = 1,  # minimum number of messages to be considered active
    period: str = "days",  # options: days, weeks, months
    slices: int = 7,  # how many chunks are relevant
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:
    min_msg_count = max(min(min_msg_count, 5), 1)
    if period not in PERIOD_OPTIONS:
        raise ValueError(f"Invalid period: {period}. Must be one of {PERIOD_OPTIONS}.")
    slices = max(min(slices, 30), 1)

    org_where = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f"AND p.organization_id = '{organization_id}'"
    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON m.created_by = u.id AND u.email NOT LIKE '%@kern.ai'
        """

    query = f"""
    WITH params AS (
    SELECT
        {min_msg_count}    AS min_msg,        -- active >= 
        {slices}           AS n,              -- slices
        '{period}'::text   AS period        -- 'days' | 'weeks' | 'months'
    ),
    periods AS (
        SELECT
        (generate_series(
            date_trunc(p.period, CURRENT_DATE)
            - (p.n - 1) * ( '1 ' || p.period )::interval,
            date_trunc(p.period, CURRENT_DATE),
            ( '1 ' || p.period )::interval
        ))::date    AS period_start,
        p.period
        FROM params p
    ),
    filtered_messages AS (
        SELECT
            p.organization_id,
            m.created_by,
            date_trunc(pa.period, m.created_at)::date AS period_start
        FROM cognition.message m
        {filter_join}
        INNER JOIN cognition.project p
            ON m.project_id = p.id {org_where}
        INNER JOIN params pa
        ON  m.created_at >= (SELECT MIN(period_start) FROM periods)
        AND m.created_at <  (SELECT MAX(period_start) + ( '1 ' || pa.period )::interval
                                FROM periods, params)
    )
    SELECT 
        o.name organization_name,
        period_start,
        (period_start + ( '1 ' || pa.period  )::INTERVAL  - '1 day'::interval  )::date AS period_end,
        active_user_count
    FROM (
        SELECT 
            organization_id,
            period_start,
            COUNT(*) active_user_count
        FROM (
            SELECT 
                organization_id,
                period_start,
                created_by,
                COUNT(*) cnt
            FROM filtered_messages M
            GROUP BY 1,2,3
            HAVING COUNT(*) >= (SELECT MAX(min_msg) FROM params)
        ) x
        GROUP BY 1,2
    )y
    INNER JOIN organization o
        ON y.organization_id = o.id
    , params pa
    ORDER BY 1,2 DESC
    """
    if as_query:
        return query
    return general.execute_all(query)


def __get_active_users_global(
    min_msg_count: int = 1,  # minimum number of messages to be considered active
    period: str = "days",  # options: days, weeks, months
    slices: int = 7,  # how many chunks are relevant
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:
    # includes type check for sql injection prevention
    min_msg_count = max(min(min_msg_count, 5), 1)
    if period not in PERIOD_OPTIONS:
        raise ValueError(f"Invalid period: {period}. Must be one of {PERIOD_OPTIONS}.")
    slices = max(min(slices, 30), 1)

    org_where = ""
    org_select = ""
    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f"AND p.organization_id = '{organization_id}'"
        org_select = f"(SELECT MAX(NAME) FROM organization WHERE id = '{organization_id}') organization_name,"

    filter_join = ""
    if without_kern_email:
        filter_join = """
        INNER JOIN PUBLIC.user u
            ON m.created_by = u.id AND u.email NOT LIKE '%@kern.ai'
        """

    query = f"""
    WITH params AS (
    SELECT
        {min_msg_count}    AS min_msg,        -- active >= 
        {slices}           AS n,              -- slices
        '{period}'::text   AS period        -- 'days' | 'weeks' | 'months'
    ),
    periods AS (
        SELECT
        (generate_series(
            date_trunc(p.period, CURRENT_DATE)
            - (p.n - 1) * ( '1 ' || p.period )::interval,
            date_trunc(p.period, CURRENT_DATE),
            ( '1 ' || p.period )::interval
        ))::date    AS period_start,
        p.period
        FROM params p
    ),
    filtered_messages AS (
        SELECT
            p.organization_id,
            m.created_by,
            date_trunc(pa.period, m.created_at)::date AS period_start
        FROM cognition.message m
        {filter_join}
        INNER JOIN cognition.project p
            ON m.project_id = p.id {org_where}
        INNER JOIN params pa
            ON  m.created_at >= (SELECT MIN(period_start) FROM periods)
            AND m.created_at <  (SELECT MAX(period_start) + ( '1 ' || pa.period )::interval
                                FROM periods, params)
    )
    SELECT 
        {org_select}
        period_start,
        (period_start + ( '1 ' || pa.period  )::INTERVAL  - '1 day'::interval  )::date AS period_end,
        active_user_count
    FROM (
        SELECT 
            period_start,
            COUNT(*) active_user_count
        FROM (
            SELECT 
                period_start,
                created_by,
                COUNT(*) cnt
            FROM filtered_messages M
            GROUP BY 1,2
            HAVING COUNT(*) >= (SELECT MAX(min_msg) FROM params)
        ) x
        GROUP BY 1
    )y	, params pa
    ORDER BY period_start DESC
"""
    if as_query:
        return query
    return general.execute_all(query)


def __get_users_by_org(
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:
    where_add = ""

    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        where_add = f"WHERE o.id = '{organization_id}'"
    if without_kern_email:
        if where_add:
            where_add += " AND"
        else:
            where_add = "WHERE"
        where_add += " u.email NOT LIKE '%@kern.ai'"

    query = f"""
    SELECT o.name, u.role, COUNT(*)
    FROM PUBLIC.user u
    INNER JOIN organization o
        ON u.organization_id = o.id
    {where_add}
    GROUP BY 1,2
"""
    if as_query:
        return query
    return general.execute_all(query)


def __get_users_to_projects(
    organization_id: str = "",
    without_kern_email: bool = False,
    as_query: bool = False,
) -> List[Row]:

    org_where = "u.organization_id IS NOT NULL"
    where_add = ""

    if organization_id:
        organization_id = prevent_sql_injection(
            organization_id, isinstance(organization_id, str)
        )
        org_where = f"u.organization_id = '{organization_id}'"
        where_add = f"WHERE o.id = '{organization_id}'"

    user_where = ""
    if without_kern_email:
        user_where = "AND u.email NOT LIKE '%@kern.ai'"

    query = f"""
    WITH user_lookup AS (
        SELECT
            u.organization_id,
            COALESCE(t.project_id::TEXT,'{ENGINEERING_TEAM_INDICATOR}') ind, 
            u.role, 
            count(DISTINCT u.id) c
        FROM public.user u
        LEFT JOIN (
            SELECT
                tr.resource_id project_id,
                tm.user_id
            FROM team t
            LEFT JOIN team_member tm
                ON t.id = tm.team_id
            LEFT JOIN team_resource tr
                ON t.id = tr.team_id AND tr.resource_type = '{enums.TeamResourceType.COGNITION_PROJECT.value}'  
            -- only users that are still annotators
            INNER JOIN PUBLIC.user u
                ON tm.user_id = u.id AND u.role = '{enums.UserRoles.ANNOTATOR.value}'
        ) t
        ON u.id = t.user_id
        WHERE NOT (t.project_id IS NULL AND u.role = '{enums.UserRoles.ANNOTATOR.value}')
        AND NOT u.role = '{enums.UserRoles.EXPERT.value}'
        AND {org_where}
        {user_where}
        GROUP BY 1,2,3
    )

    SELECT *
    FROM (
        SELECT 
            o.name organization_name,
            p.name project_or_engineers,
            COALESCE(ul.role,'{enums.UserRoles.ANNOTATOR.value}') "role",
            COALESCE(ul.c, 0) "count"
        FROM organization o
        LEFT JOIN cognition.project p
            ON o.id = p.organization_id
        LEFT JOIN user_lookup ul
            ON o.id = ul.organization_id AND (p.id::TEXT = ind AND ind != '{ENGINEERING_TEAM_INDICATOR}')
        {where_add}
        UNION ALL 
        SELECT 
            o.name,
            '{ENGINEERING_TEAM_INDICATOR}',
            COALESCE(ul.role,'{enums.UserRoles.ENGINEER.value}') "role",
            COALESCE(ul.c, 0) "count"
        FROM organization o
        LEFT JOIN user_lookup ul
            ON o.id = ul.organization_id AND ind = '{ENGINEERING_TEAM_INDICATOR}' 
        {where_add}
    )x
    WHERE project_or_engineers IS NOT NULL
    ORDER BY organization_name,
        CASE WHEN project_or_engineers = '{ENGINEERING_TEAM_INDICATOR}' THEN '000000' 
        ELSE project_or_engineers END,
        role
"""
    if as_query:
        return query
    return general.execute_all(query)
