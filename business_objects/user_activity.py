from datetime import datetime
import traceback
from typing import List, Any
from submodules.model import models

from submodules.model.business_objects import user

from .. import UserActivity
from ..business_objects import general
from ..session import session

from sqlalchemy import sql


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


def update_last_interaction(user_id: str) -> None:
    user_item = user.get(user_id)
    user_item.last_interaction = sql.func.now()
    general.commit()


def get_active_users_in_range(
    last_interaction_range: datetime, order_by_interaction: bool
) -> models.User:
    query = session.query(models.User)

    if last_interaction_range:
        query = session.query(models.User).filter(
            models.User.last_interaction >= (last_interaction_range),
        )

    if order_by_interaction:
        query = query.order_by(models.User.last_interaction.desc())

    return query.all()


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


def delete_user_activity(user_id: str, with_commit: bool = False) -> None:
    query = f"DELETE FROM user_activity WHERE created_by = '{user_id}'"
    general.execute(query)
    general.flush_or_commit(with_commit)
