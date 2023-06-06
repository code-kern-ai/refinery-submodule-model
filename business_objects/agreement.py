from datetime import datetime
from typing import Optional
from submodules.model import session
from submodules.model.business_objects import general
from submodules.model.models import Agreement


def get(project_id: str, item_id: str) -> Agreement:
    return (
        session.query(Agreement)
        .filter(Agreement.project_id == project_id, Agreement.id == item_id)
        .first()
    )

def create(
    project_id: str,
    user_id: str,
    terms_text: str,
    terms_accepted: bool,
    xfkey: Optional[str] = None,
    xftype: Optional[str] = None,
    with_commit: bool = True,
) -> Agreement:
    
    agreement = Agreement(
        project_id=project_id, user_id=user_id, xfkey=xfkey, xftype=xftype, terms_text=terms_text, terms_accepted=terms_accepted
    )

    general.add(agreement, with_commit)
    return agreement