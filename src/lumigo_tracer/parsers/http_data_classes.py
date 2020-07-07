from copy import deepcopy


class HttpRequest:
    host: str
    method: str
    uri: str
    headers: dict
    body: bytes

    def __init__(self, **kwargs):
        self.host = kwargs["host"]
        self.method = kwargs["method"]
        self.uri = kwargs["uri"]
        self.headers = {k.lower(): v for k, v in (kwargs.get("headers") or {}).items()}
        self.body = kwargs.get("body")

    def clone(self, **kwargs):
        clone_obj = deepcopy(self)
        for k, v in kwargs.items():
            setattr(clone_obj, k, v)
        return clone_obj
