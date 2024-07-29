import uuid
from datetime import date
from ..enums import StrategyComplexity
from . import project
from ..business_objects import general
from typing import Optional
from ..util import prevent_sql_injection
from submodules.model.cognition_objects import (
    macro as macro_db_co,
)


def log_macro_execution_summary(
    macro_id: str,
    with_commit: bool = True,
):
    id = str(uuid.uuid4())

    macro_object = macro_db_co.get(macro_id)
    org_id = macro_object.organization_id
    macro_type = macro_object.macro_type

    query = f"""
    INSERT INTO cognition.macro_execution_summary (id, organization_id, creation_month, macro_type, count)
    VALUES ('{id}', '{org_id}', date_trunc('month', '{date.today()}'), '{macro_type}', 1)
    ON CONFLICT ON CONSTRAINT unique_summary DO UPDATE
        SET count = macro_execution_summary.count + 1;
    """
    general.execute(query)
    general.flush_or_commit(with_commit)
