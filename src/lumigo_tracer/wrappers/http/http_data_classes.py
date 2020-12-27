from copy import deepcopy
from typing import Optional, List


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


class HttpState:
    previous_request: Optional[HttpRequest] = None
    previous_response_body: bytes = b""
    omit_skip_path: Optional[List[str]] = None

    @staticmethod
    def clear():
        HttpState.previous_request = None
        HttpState.previous_response_body = b""
