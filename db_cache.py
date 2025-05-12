import time
import functools
import inspect
import threading
from enum import Enum
from .daemon import run_without_db_token


# Enum for logical cache separation\
class CacheEnum(Enum):
    DEFAULT = "default"
    USER = "user"
    ORGANIZATION = "organization"
    PROJECT = "project"
    TEAM = "team"
    # extend with more categories as needed


# Global cache map: each cache_type -> its own dict of key -> (value, expires_at)
_GLOBAL_CACHE_MAP = {}
# Lock to protect cache operations
_CACHE_LOCK = threading.Lock()


def _cleanup_expired():
    while True:
        time.sleep(60 * 60)  # run every hour
        now = time.time()
        with _CACHE_LOCK:
            for cache in _GLOBAL_CACHE_MAP.values():
                # collect expired keys first
                expired_keys = [key for key, (_, exp) in cache.items() if now >= exp]
                for key in expired_keys:
                    del cache[key]


# # Start cleanup thread as daemon
# _cleanup_thread = threading.Thread(target=_cleanup_expired, daemon=True)
# _cleanup_thread.start()


def start_cleanup_thread():
    run_without_db_token(_cleanup_expired)


class TTLCacheDecorator:
    def __init__(self, cache_type=CacheEnum.DEFAULT, ttl_minutes=None, *key_fields):
        """
        cache_type: namespace for the cache
        ttl_minutes: time-to-live for cache entries, in minutes
        key_fields: argument names (str) to build cache key; positions are not supported
        """
        if not isinstance(cache_type, CacheEnum):
            raise TypeError("cache_type must be a CacheEnum member")
        if ttl_minutes is None:
            raise ValueError("ttl_minutes must be specified")
        self.cache_type = cache_type
        # convert minutes to seconds
        self.ttl = ttl_minutes * 60
        # only named fields
        for f in key_fields:
            if not isinstance(f, str):
                raise TypeError("key_fields must be argument names (strings)")
        self.key_fields = key_fields

    def __call__(self, fn):
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            bound = sig.bind_partial(*args, **kwargs)
            # build cache key tuple from named fields
            try:
                key = tuple(bound.arguments[field] for field in self.key_fields)
            except KeyError as e:
                raise KeyError(f"Missing argument for cache key: {e}")

            now = time.time()
            with _CACHE_LOCK:
                cache = _GLOBAL_CACHE_MAP.setdefault(self.cache_type, {})
                entry = cache.get(key)
                if entry:
                    value, expires_at = entry
                    if now < expires_at:
                        # print(f"Cache hit for {key} in {self.cache_type}")
                        return value
                    # expired
                    del cache[key]

            # miss or expired
            # print(f"No cache hit for {key} in {self.cache_type}")
            result = fn(*args, **kwargs)
            with _CACHE_LOCK:
                cache = _GLOBAL_CACHE_MAP[self.cache_type]
                cache[key] = (result, now + self.ttl)
            return result

        # management methods
        def invalidate(**kws):
            try:
                key = tuple(kws[field] for field in self.key_fields)
            except KeyError as e:
                raise KeyError(f"Missing argument for invalidate key: {e}")
            with _CACHE_LOCK:
                _GLOBAL_CACHE_MAP.get(self.cache_type, {}).pop(key, None)

        def update(value, **kws):
            try:
                key = tuple(kws[field] for field in self.key_fields)
            except KeyError as e:
                raise KeyError(f"Missing argument for update key: {e}")
            with _CACHE_LOCK:
                cache = _GLOBAL_CACHE_MAP.setdefault(self.cache_type, {})
                cache[key] = (value, time.time() + self.ttl)

        def clear_all():
            with _CACHE_LOCK:
                _GLOBAL_CACHE_MAP[self.cache_type] = {}

        wrapper.invalidate = invalidate
        wrapper.update = update
        wrapper.clear_all = clear_all
        wrapper.ttl = self.ttl
        wrapper.key_fields = self.key_fields
        wrapper.cache_type = self.cache_type

        return wrapper


# ─── GLOBAL INVALIDATE / UPDATE ──────────────────────────────────────────────


def invalidate_cache(cache_type: CacheEnum, key: tuple):
    """
    Remove a single entry from the given cache.
    key must be the exact tuple used when caching.
    """
    if not isinstance(cache_type, CacheEnum):
        raise TypeError("cache_type must be a CacheEnum member")
    with _CACHE_LOCK:
        _GLOBAL_CACHE_MAP.get(cache_type, {}).pop(key, None)


def update_cache(cache_type: CacheEnum, key: tuple, value, ttl_minutes: float):
    """
    Force-set a value in cache under `cache_type` and `key`, overriding any existing entry.
    """
    if not isinstance(cache_type, CacheEnum):
        raise TypeError("cache_type must be a CacheEnum member")
    expires_at = time.time() + ttl_minutes * 60
    with _CACHE_LOCK:
        cache = _GLOBAL_CACHE_MAP.setdefault(cache_type, {})
        cache[key] = (value, expires_at)


# Example usage: !Note the tuples syntax
# invalidate_cache(CacheEnum.USER, (user_id,))
# update_cache(CacheEnum.USER, (user_id,), some_value, ttl_minutes=5)


# Example usage:
# @TTLCacheDecorator(CacheEnum.USER, 60, 'user_id')
# def get_user_by_id(user_id):
#     print(f"Fetching user {user_id} from database")
#     return {"id": user_id, "name": "John"}

# @TTLCacheDecorator(CacheEnum.USER, 60, 'user_id')
# def get_admin_user(user_id, dummy=None):
#     print(f"Fetching admin user {user_id} from database")
#     return {"id": user_id, "role": "admin"}

# @TTLCacheDecorator(CacheEnum.RECORD, 120, 'project_id', 'record_id')
# def get_record(project_id, record_id):
#     print(f"Fetching record {project_id}, {record_id} from database")
#     return {"project_id": project_id, "id": record_id, "value": "Some data"}

# Management examples:
# get_user_by_id.invalidate(user_id=1)
# get_user_by_id.update({"id":1, "name":"Jane"}, user_id=1)
# get_user_by_id.clear_all()
# get_record.clear_all()
