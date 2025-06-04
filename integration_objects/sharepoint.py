from typing import Optional
from datetime import datetime
from sqlalchemy import func

import pytz

from submodules.model.session import session
from submodules.model.models import CognitionIntegration, IntegrationSharepoint


def get_modified_since(integration: CognitionIntegration) -> Optional[datetime]:
    modified = (
        session.query(func.max(IntegrationSharepoint.modified))
        .filter(IntegrationSharepoint.integration_id == integration.id)
        .first()
    )[0] or datetime(1970, 1, 1)
    return pytz.UTC.localize(modified)
