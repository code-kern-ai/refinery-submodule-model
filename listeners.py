from sqlalchemy import event
from datetime import datetime, timezone
from submodules.model.models import CognitionConversation, CognitionMessage
import traceback

from src.controller.admin_query_message_summary import (
    manager as admin_query_message_summary_manager,
)
from submodules.model.business_objects import general


@event.listens_for(CognitionConversation, "after_insert")
def after_insert(mapper, connection, conversation_entity: CognitionConversation):
    try:
        session_token = general.get_ctx_token()
        admin_query_message_summary_manager.log_conversation_summary(
            conversation_entity, 1
        )
    except Exception:
        print(traceback.format_exc(), flush=True)
    finally:
        general.remove_and_refresh_session(session_token)


@event.listens_for(CognitionMessage, "after_insert")
def after_insert(mapper, connection, message_entity: CognitionMessage):
    try:
        session_token = general.get_ctx_token()
        admin_query_message_summary_manager.log_message_summary(message_entity)
    except Exception:
        print("Error in after_insert listener of CognitionMessage", flush=True)
        print(traceback.format_exc(), flush=True)
    finally:
        general.remove_and_refresh_session(session_token)
