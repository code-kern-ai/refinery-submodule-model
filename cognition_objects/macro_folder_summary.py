import uuid
from datetime import date
from ..business_objects import general
from submodules.model.cognition_objects import (
    macro as macro_db_co,
)


def log_macro_execution_summary(
    org_id: str,
    macro_type: str,
    processed_files_number: int,
    with_commit: bool = True,
):
    id = str(uuid.uuid4())
    query = f"""
    INSERT INTO cognition.macro_execution_summary (id, organization_id, creation_month, macro_type, execution_count, processed_files_count)
    VALUES ('{id}', '{org_id}', date_trunc('month', '{date.today()}'::timestamp), '{macro_type}', 1, {processed_files_number})
    ON CONFLICT ON CONSTRAINT unique_macro_summary DO UPDATE
        SET
            execution_count = macro_execution_summary.execution_count + 1,
            processed_files_count = macro_execution_summary.processed_files_count + {processed_files_number};
    """
    general.execute(query)
    general.flush_or_commit(with_commit)
