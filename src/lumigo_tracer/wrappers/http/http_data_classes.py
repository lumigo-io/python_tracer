from copy import deepcopy
from typing import Optional, List, Dict


class HttpRequest:
    host: str
    method: str
    uri: str
    headers: dict  # type: ignore[type-arg]
    body: bytes
    instance_id: Optional[bytes]

    def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
        self.host = kwargs["host"]
        self.method = kwargs["method"]
        self.uri = kwargs["uri"]
        self.headers = {k.lower(): v for k, v in (kwargs.get("headers") or {}).items()}
        self.body = kwargs.get("body")  # type: ignore
        self.instance_id = kwargs.get("instance_id")

    def clone(self, **kwargs):  # type: ignore[no-untyped-def]
        clone_obj = deepcopy(self)
        for k, v in kwargs.items():
            setattr(clone_obj, k, v)
        return clone_obj


class HttpState:
    previous_request: Optional[HttpRequest] = None
    previous_span_id: Optional[str] = None
    omit_skip_path: Optional[List[str]] = None
    request_id_to_span_id: Dict[int, str] = {}
    response_id_to_span_id: Dict[int, str] = {}

    @staticmethod
    def clear():  # type: ignore[no-untyped-def]
        HttpState.previous_request = None
        HttpState.request_id_to_span_id.clear()
        HttpState.response_id_to_span_id.clear()
