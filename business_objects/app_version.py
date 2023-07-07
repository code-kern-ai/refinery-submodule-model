from typing import List, Optional
from datetime import datetime
from ..models import AppVersion
from ..session import session
from ..business_objects import general


def get_by_name(service: str) -> AppVersion:
    return session.query(AppVersion).filter(AppVersion.service == service).first()


def get_all() -> List[AppVersion]:
    return session.query(AppVersion).all()


def create(
    service: str,
    installed_version: Optional[str] = None,
    remote_version: Optional[str] = None,
    last_checked: Optional[datetime] = None,
    with_commit: bool = False,
) -> AppVersion:
    app_version = AppVersion(service=service)

    if installed_version:
        app_version.installed_version = installed_version

    if remote_version:
        app_version.remote_version = remote_version

    if last_checked:
        app_version.last_checked = last_checked

    general.add(app_version, with_commit)
    return app_version
