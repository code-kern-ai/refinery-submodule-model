from submodules.model.models import AdminMessage
from . import general
from ..session import session


def get(message_id):
    return session.query(AdminMessage).filter(AdminMessage.id == message_id).first()


def get_all():
    return session.query(AdminMessage).filter().all()


def get_all_active():
    return session.query(AdminMessage).filter(AdminMessage.archived == False).all()


def create(text: str, level: str, archive_date: int, created_by: str):
    message = AdminMessage(
        text=text,
        level=level,
        archive_date=archive_date,
        created_by=created_by,
    )
    general.add(message, True)


def archive(message_id: str, archived_by: str, archive_date):
    message = get(message_id)
    message.archived = True
    message.archived_by = archived_by
    message.archive_date = archive_date
    general.commit()
