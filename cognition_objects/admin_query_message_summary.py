from datetime import date
from submodules.model.business_objects import general
from submodules.model.models import AdminQueryMessageSummary
from ..session import session


def get_admin_query_message_summary(
    org_id: str,
    project_id: str,
    day: date,
) -> AdminQueryMessageSummary | None:
    return (
        session.query(AdminQueryMessageSummary)
        .filter(
            AdminQueryMessageSummary.organization_id == org_id,
            AdminQueryMessageSummary.project_id == project_id,
            AdminQueryMessageSummary.day == day,
        )
        .first()
    )


def log_admin_query_message_summary(
    org_id: str,
    project_id: str,
    day: date,
    counters: dict,  # {"total_messages": 5, "messages_via_api": 2, ....},
    with_commit: bool = True,
):

    message_summary = get_admin_query_message_summary(org_id, project_id, day)

    if message_summary:
        for col, value in counters.items():
            if not hasattr(message_summary, col):
                continue
            current_value = getattr(message_summary, col, 0) or 0
            setattr(message_summary, col, current_value + value)
    else:
        message_summary = AdminQueryMessageSummary(
            organization_id=org_id, project_id=project_id, day=day
        )
        for col, value in counters.items():
            if hasattr(message_summary, col):
                setattr(message_summary, col, value)
        general.add(message_summary, with_commit)
        return message_summary

    if with_commit:
        general.flush_or_commit(True)
    return message_summary
