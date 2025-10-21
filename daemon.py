import threading
import traceback
from submodules.model.business_objects import general
from submodules.model import telemetry


def run_without_db_token(target, *args, **kwargs):
    """
    DB session token isn't automatically created.
    You can still do this with general.get_ctx_token but need to return it yourself with remove_and_refresh_session.
    """
    fn_name = f"{target.__module__}.{target.__name__}"

    def wrapper():
        telemetry.TASK_RUNNING.labels(
            app_name=telemetry.APP_NAME,
            task_name=fn_name,
        ).inc()

        try:
            target(*args, **kwargs)
        except Exception:
            telemetry.TASK_ERRORS.labels(
                app_name=telemetry.APP_NAME,
                task_name=fn_name,
            ).inc()
            print("=== Exception in thread ===", flush=True)
            print(traceback.format_exc(), flush=True)
            print("===========================", flush=True)
        else:
            telemetry.TASK_PROCESSED.labels(
                app_name=telemetry.APP_NAME,
                task_name=fn_name,
            ).inc()
        finally:
            telemetry.TASK_RUNNING.labels(
                app_name=telemetry.APP_NAME,
                task_name=fn_name,
            ).dec()

    threading.Thread(
        target=wrapper,
        daemon=True,
    ).start()


def run_with_db_token(target, *args, **kwargs):
    """
    DB session token is automatically created & returned at the end.
    Long running threads needs to occasionally daemon.reset_session_token_in_thread to ensure the session doesn't get a timeout.
    """
    fn_name = f"{target.__module__}.{target.__name__}"

    # this is a workaround to set the token in the actual thread context
    def wrapper():
        general.get_ctx_token()
        telemetry.TASK_RUNNING.labels(
            app_name=telemetry.APP_NAME,
            task_name=fn_name,
        ).inc()

        try:
            target(*args, **kwargs)
        except Exception:
            telemetry.TASK_ERRORS.labels(
                app_name=telemetry.APP_NAME,
                task_name=fn_name,
            ).inc()
            print("=== Exception in thread ===", flush=True)
            print(traceback.format_exc(), flush=True)
            print("===========================", flush=True)
        else:
            telemetry.TASK_PROCESSED.labels(
                app_name=telemetry.APP_NAME,
                task_name=fn_name,
            ).inc()
        finally:
            general.remove_and_refresh_session()
            telemetry.TASK_RUNNING.labels(
                app_name=telemetry.APP_NAME,
                task_name=fn_name,
            ).dec()

    threading.Thread(
        target=wrapper,
        daemon=True,
    ).start()


def prepare_thread(target, *args, **kwargs) -> threading.Thread:
    return threading.Thread(
        target=target,
        args=args,
        kwargs=kwargs,
        daemon=True,
    )
