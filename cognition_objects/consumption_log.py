from ..models import CognitionConsumptionLog
from ..business_objects import general
from .. import enums


def create(
    organization_id: str,
    project_id: str,
    strategy_id: str,
    conversation_id: str,
    message_id: str,
    created_by: str,
    complexity: enums.StrategyComplexity,
    state: enums.ConsumptionLogState,
    project_state: enums.CognitionProjectState,
    with_commit: bool = True,
) -> CognitionConsumptionLog:
    consumption_log = CognitionConsumptionLog(
        organization_id=organization_id,
        project_id=project_id,
        strategy_id=strategy_id,
        conversation_id=conversation_id,
        message_id=message_id,
        created_by=created_by,
        complexity=complexity.value,
        state=state.value,
        project_state=project_state.value,
    )
    general.add(consumption_log, with_commit=with_commit)
    return consumption_log
