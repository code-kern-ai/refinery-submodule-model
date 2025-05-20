import asyncio
from .business_objects import general
from contextvars import copy_context
from .session import session
import functools
from contextlib import contextmanager, asynccontextmanager


def _run_with_session(fn, *args, new_session: bool = True, **kwargs):
    """
    Sync helper: ensures a request-id is set (or reset), runs fn(*args, **kwargs),
    then optionally removes the session.

    Args:
        fn: the DB function to run
        new_session: if True, always assign a fresh UUID as the request ID
    """

    if new_session:
        general.get_ctx_token()

    try:
        return fn(*args, **kwargs)
    except Exception:
        session.rollback()
        raise
    finally:
        if new_session:
            general.remove_and_refresh_session()


def with_session():
    """
    Decorator for sync DB functions.

    Args:
        auto_remove: session.remove() after fn returns (default True)
    """

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return _run_with_session(
                fn,
                *args,
                **kwargs,
            )

        return wrapper

    return decorator


async def run_db_async_with_session(fn, *args, new_session: bool = True, **kwargs):
    """
    Async helper: runs a sync @with_session function in a threadpool.

    Args:
        fn: the @with_session-decorated function to call
        auto_remove: pass-through to control session removal
        new_session: pass-through to force fresh session UUID
    """
    ctx = copy_context()

    def call():
        return ctx.run(
            _run_with_session,
            fn,
            *args,
            new_session=new_session,
            **kwargs,
        )

    return await asyncio.to_thread(call)


async def run_async(fn, *args, **kwargs):
    """
    Async helper: runs a sync function in a threadpool.

    Args:
        fn: the function to call
    """
    ctx = copy_context()

    def call():
        return ctx.run(fn, *args, **kwargs)

    return await asyncio.to_thread(call)


@contextmanager
def session_on_demand():

    try:
        general.get_ctx_token()
        yield

    except Exception:
        session.rollback()
        raise
    finally:
        general.remove_and_refresh_session()


@asynccontextmanager
async def async_session_on_demand():

    general.get_ctx_token()
    ctx = copy_context()
    try:
        yield
    except Exception:
        await asyncio.to_thread(session.rollback)
        raise
    finally:
        await asyncio.to_thread(lambda: ctx.run(general.remove_and_refresh_session))
