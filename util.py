import os
from typing import Tuple


def collect_engine_variables() -> Tuple[int, int, bool, bool]:
    # amount of simultaneous connections to the database
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_size
    pool_size = 20
    os_pool_size = os.getenv("POSTGRES_POOL_SIZE")
    if os_pool_size:
        try:
            pool_size = int(os_pool_size)
        except ValueError:
            print(
                f"POSTGRES_POOL_SIZE is not an integer, using default {pool_size}",
                flush=True,
            )
    # Recycle connections after x seconds. This is only done on checkout not "always"
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_recycle
    # https://docs.sqlalchemy.org/en/20/core/pooling.html#setting-pool-recycle
    pool_recycle = 3600
    os_pool_recycle = os.getenv("POSTGRES_POOL_RECYCLE")
    if os_pool_recycle:
        try:
            pool_recycle = int(os_pool_recycle)
        except ValueError:
            print(
                f"POSTGRES_POOL_RECYCLE is not an integer, using default {pool_recycle}",
                flush=True,
            )
    # use LIFO instead of FIFO (stack vs queue)
    # https://docs.sqlalchemy.org/en/20/core/pooling.html#using-fifo-vs-lifo
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_use_lifo
    pool_use_lifo = False
    os_pool_use_lifo = os.getenv("POSTGRES_POOL_USE_LIFO")
    if os_pool_use_lifo:
        try:
            pool_use_lifo = os_pool_use_lifo.lower() in ["true", "x", "1", "y"]
        except ValueError:
            print(
                f"POSTGRES_POOL_USE_LIFO is not an boolean, using default {pool_use_lifo}",
                flush=True,
            )

    # test connections on checkout - results in a ping to the database (so small overhead per request) but ensures that the connection is still alive
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_pre_ping
    # https://docs.sqlalchemy.org/en/20/core/pooling.html#disconnect-handling-pessimistic
    pool_pre_ping = True
    os_pool_pre_ping = os.getenv("POSTGRES_POOL_PRE_PING")
    if os_pool_pre_ping:
        try:
            pool_pre_ping = os_pool_pre_ping.lower() in ["true", "x", "1", "y"]
        except ValueError:
            print(
                f"POSTGRES_POOL_PRE_PING is not an boolean, using default {pool_pre_ping}",
                flush=True,
            )

    # overflow of pool limit, -1 = infinite (shouldn't be used)
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.max_overflow
    # https://docs.sqlalchemy.org/en/20/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow

    pool_max_overflow = 10
    os_pool_max_overflow = os.getenv("POSTGRES_POOL_MAX_OVERFLOW")
    if os_pool_max_overflow:
        try:
            pool_max_overflow = int(os_pool_max_overflow)
        except ValueError:
            print(
                f"POSTGRES_POOL_MAX_OVERFLOW is not an integer, using default {pool_max_overflow}",
                flush=True,
            )

    return pool_size, pool_max_overflow, pool_recycle, pool_use_lifo, pool_pre_ping
