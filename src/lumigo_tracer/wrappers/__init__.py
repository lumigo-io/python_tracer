from .http.sync_http_wrappers import wrap_http_calls
from .pymongo.pymongo_wrapper import wrap_pymongo
from .redis.redis_wrapper import wrap_redis
from .sql.sqlalchemy_wrapper import wrap_sqlalchemy
from .aiohttp.aiohttp_wrapper import wrap_aiohttp


already_wrapped = False


def wrap(force: bool = False):
    global already_wrapped
    if not already_wrapped:
        # Never wrap http calls twice - it will create duplicate body
        wrap_http_calls()
    if force or not already_wrapped:
        wrap_pymongo()
        wrap_redis()
        wrap_sqlalchemy()
        wrap_aiohttp()
        already_wrapped = True
