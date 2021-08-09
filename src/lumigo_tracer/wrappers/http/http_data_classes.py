from copy import deepcopy
from typing import Optional, List, Dict


class HttpRequest:
    host: str
    method: str
    uri: str
    headers: dict
    body: bytes
    instance_id: Optional[bytes]

    def __init__(self, **kwargs):
        self.host = kwargs["host"]
        self.method = kwargs["method"]
        self.uri = kwargs["uri"]
        self.headers = {k.lower(): v for k, v in (kwargs.get("headers") or {}).items()}
        self.body = kwargs.get("body")
        self.instance_id = kwargs.get("instance_id")

    def clone(self, **kwargs):
        clone_obj = deepcopy(self)
        for k, v in kwargs.items():
            setattr(clone_obj, k, v)
        return clone_obj


class HttpState:
    previous_request: Optional[HttpRequest] = None
    previous_response_body: bytes = b""
    previous_span_id: Optional[str] = None
    omit_skip_path: Optional[List[str]] = None
    request_to_span_id: Dict[int, str] = {}
    response_to_span_id: Dict[int, str] = {}

    @staticmethod
    def clear():
        HttpState.previous_request = None
        HttpState.previous_response_body = b""
        HttpState.request_to_span_id.clear()
        HttpState.response_to_span_id.clear()
