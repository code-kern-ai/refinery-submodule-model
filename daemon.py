import threading
from submodules.model.business_objects import general
from contextvars import ContextVar
import traceback

thread_session_token = ContextVar("token", default=None)


def run(target, *args, **kwargs):
    """
    DB session token isn't automatically created.
    You can still do this with general.get_ctx_token but need to return it yourself with remove_and_refresh_session.
    """
    threading.Thread(
        target=target,
        args=args,
        kwargs=kwargs,
        daemon=True,
    ).start()


def run_with_db_token(target, *args, **kwargs):
    """
    DB session token is automatically created & returned at the end.
    Long running threads needs to occasionally daemon.reset_session_token_in_thread to ensure the session doesn't get a timeout.
    """

    # this is a workaround to set the token in the actual thread context
    def wrapper():
        token = general.get_ctx_token()
        thread_session_token.set(token)
        try:
            target(*args, **kwargs)
        except Exception:
            print("=== Exception in thread ===", flush=True)
            print(traceback.format_exc(), flush=True)
            print("===========================", flush=True)
        finally:
            reset_session_token_in_thread(False)

    threading.Thread(
        target=wrapper,
        daemon=True,
    ).start()


def reset_session_token_in_thread(request_new: bool = True):
    token = thread_session_token.get()
    if not token:
        # shouldn't happen if used with the run_with_session_token function
        # so we print where it was called from
        print(traceback.format_stack())
        raise ValueError("No token set in thread context")
    new_token = general.remove_and_refresh_session(token, request_new)
    if new_token:
        thread_session_token.set(new_token)


def prepare_thread(target, *args, **kwargs) -> threading.Thread:
    return threading.Thread(
        target=target,
        args=args,
        kwargs=kwargs,
        daemon=True,
    )
