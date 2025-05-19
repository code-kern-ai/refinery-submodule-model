# db_utils.py

import asyncio

# import uuid
from .business_objects import general
from contextvars import copy_context
from .session import session, request_id_ctx_var
import functools
from contextlib import contextmanager


def _run_with_session(
    fn, *args, auto_remove: bool = True, new_session: bool = True, **kwargs
):
    """
    Sync helper: ensures a request-id is set (or reset), runs fn(*args, **kwargs),
    then optionally removes the session.

    Args:
        fn: the DB function to run
        auto_remove: if True, calls Session.remove() after execution
        new_session: if True, always assign a fresh UUID as the request ID
    """
    # decide on request ID behavior
    if new_session or request_id_ctx_var.get() is None:
        # generate a unique request id for this session
        # request_id_ctx_var.set(str(uuid.uuid4()))
        general.get_ctx_token()

    try:
        # Scoped Session uses request_id_ctx_var under the hood
        return fn(*args, **kwargs)
    except Exception:
        session.rollback()
        raise
    finally:
        if auto_remove:
            session.remove()


def with_session(auto_remove: bool = True, new_session: bool = False):
    """
    Decorator for sync DB functions.

    Args:
        auto_remove: session.remove() after fn returns (default True)
        new_session: force a fresh session UUID for each call (default False)
    """

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return _run_with_session(
                fn,
                *args,
                auto_remove=auto_remove,
                new_session=new_session,
                **kwargs,
            )

        return wrapper

    return decorator


async def run_db(
    fn, *args, auto_remove: bool = True, new_session: bool = False, **kwargs
):
    """
    Async helper: runs a sync @with_session function in a threadpool.

    Args:
        fn: the @with_session-decorated function to call
        auto_remove: pass-through to control session removal
        new_session: pass-through to force fresh session UUID
    """
    ctx = copy_context()

    def call():
        # explicitly pass keyword-only args
        return ctx.run(
            _run_with_session,
            fn,
            *args,
            auto_remove=auto_remove,
            new_session=new_session,
            **kwargs,
        )

    return await asyncio.to_thread(call)


@contextmanager
def session_on_demand(new_session: bool = True, auto_remove: bool = True):

    try:
        if new_session or request_id_ctx_var.get() is None:
            general.get_ctx_token()
        yield

    except Exception:
        session.rollback()
        raise
    finally:
        if auto_remove:
            session.remove()

        # general.remove_and_refresh_session()
