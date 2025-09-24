from sqlalchemy import event
from datetime import datetime, timezone
from submodules.model.models import CognitionConversation, CognitionMessage
from submodules.model.session_wrapper import with_session
import traceback

from src.controller.admin_query_message_summary import (
    manager as admin_query_message_summary_manager,
)


@event.listens_for(CognitionConversation, "after_insert")
@with_session()
def after_insert(mapper, connection, conversation_entity: CognitionConversation):
    try:
        admin_query_message_summary_manager.log_conversation_summary(
            conversation_entity, 1
        )
    except Exception:
        print("Error in after_insert listener of CognitionConversation", flush=True)
        print(traceback.format_exc(), flush=True)


@event.listens_for(CognitionMessage, "after_insert")
@with_session()
def after_insert(mapper, connection, message_entity: CognitionMessage):
    try:
        admin_query_message_summary_manager.log_message_summary(message_entity)
    except Exception:
        print("Error in after_insert listener of CognitionMessage", flush=True)
        print(traceback.format_exc(), flush=True)
