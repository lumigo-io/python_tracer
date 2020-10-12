from .http.sync_http_wrappers import wrap_http_calls
from .pymongo.pymongo_wrapper import wrap_pymongo


already_wrapped = False


def wrap():
    global already_wrapped
    if not already_wrapped:
        wrap_http_calls()
        wrap_pymongo()
        already_wrapped = True
