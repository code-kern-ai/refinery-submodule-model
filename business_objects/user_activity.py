import traceback
from typing import List, Any

from .. import UserActivity
from ..business_objects import general
from ..session import session


def get_all_user_activity() -> List[Any]:
    query = """
    SELECT 
        user_id, 
        activity_feed, 
        CASE WHEN activity_feed IS NULL OR day_diff > 7  THEN TRUE ELSE FALSE END has_warning,
        CASE 
            WHEN activity_feed IS NULL THEN 'No activity found for user' 
            WHEN day_diff > 7  THEN 'Last activity > 7 ago (' || day_diff || ')' 
            ELSE NULL 
        END warning_text 
    FROM (
        SELECT id user_id,z.*
        FROM PUBLIC.user u
        LEFT JOIN (
            SELECT *, DATE_PART('day', NOW()-last_entry) day_diff
            FROM (
                SELECT created_by, array_agg(activity_data ORDER BY created_at DESC) activity_feed, MAX(created_at) last_entry
                FROM (
                    SELECT created_by, created_at, json_build_object('activity',ua.activity,'created_at',ua.created_at,'from_backup',ua.from_backup) activity_data
                    FROM user_activity ua ) x
                GROUP BY created_by )y
            )z
        ON u.id = z.created_by
    )a
    """
    return general.execute_all(query)


def write_user_activity_safe(
    entries_to_add: List[List[Any]], with_commit: bool = False
) -> None:
    ctx_token = general.get_ctx_token()

    try:
        general.add_all(
            [
                UserActivity(
                    created_by=entry[0],
                    activity=entry[1],
                    created_at=entry[2],
                    from_backup=entry[3],
                )
                for entry in entries_to_add
            ],
            with_commit,
        )
        __remove_old_user_activity_entries(100, with_commit=True)
    except:
        print(traceback.format_exc(), flush=True)
    finally:
        general.reset_ctx_token(ctx_token, True)


def __remove_old_user_activity_entries(
    keep: int = 100, with_commit: bool = True
) -> None:
    query = f"""
    DELETE 
    FROM user_activity ua 
    USING (
        SELECT id
        FROM (
            SELECT id, ROW_NUMBER () OVER ( PARTITION BY created_by ORDER BY created_at DESC)rn
            FROM user_activity ua) x
            WHERE rn > {keep}) helper
    WHERE ua.id = helper.id; """
    general.execute(query)
    general.flush_or_commit(with_commit)
