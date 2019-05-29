from http import client
from copy import deepcopy


class HttpRequest:
    host: str
    method: str
    uri: str
    headers: client.HTTPMessage
    body: bytes

    def __init__(self, **kwargs):
        self.host = kwargs["host"]
        self.method = kwargs["method"]
        self.uri = kwargs["uri"]
        self.headers = kwargs.get("headers")
        self.body = kwargs.get("body")

    def clone(self, **kwargs):
        clone_obj = deepcopy(self)
        for k, v in kwargs.items():
            setattr(clone_obj, k, v)
        return clone_obj
