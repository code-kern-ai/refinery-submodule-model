from typing import List
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import TimedExecutions
from ..enums import TimedExecutionKey
from ..models import User


def get(key: TimedExecutionKey) -> TimedExecutions:
    return (
        session.query(TimedExecutions)
        .filter(
            TimedExecutions.time_key == key.value,
        )
        .first()
    )


def get_all() -> List[TimedExecutions]:
    return session.query(TimedExecutions).all()


def execute_time_key_update(
    with_commit: bool = True,
) -> None:
    # potential optimization by only getting "needed" keys so the time check is already done in the query
    all_keys = {key for key in TimedExecutionKey}
    all_existing = {obj.time_key: obj for obj in get_all()}

    for key in all_keys:
        obj = all_existing.get(key.value)
        if obj:
            last_executed_at = obj.last_executed_at
        else:
            last_executed_at = datetime.min
        if __execute_timed_execution_by_key(key, last_executed_at):
            if obj:
                obj.last_executed_at = datetime.now()
            else:
                obj = TimedExecutions(
                    time_key=key.value,
                    last_executed_at=datetime.now(),
                )
                general.add(obj, False)
    general.flush_or_commit(with_commit)


def __execute_timed_execution_by_key(
    key: TimedExecutionKey, last_executed_at: datetime
) -> bool:
    if key == TimedExecutionKey.LAST_RESET_USER_MESSAGE_COUNT:
        # check if month has changed since last execution
        now = datetime.now()
        if last_executed_at.year == now.year and last_executed_at.month == now.month:
            return False  # already executed this month

        session.query(User).update({User.messages_created_this_month: 0})
        general.flush_or_commit(False)
        return True

    raise NotImplementedError(f"Timed execution for key {key} is not implemented yet")
