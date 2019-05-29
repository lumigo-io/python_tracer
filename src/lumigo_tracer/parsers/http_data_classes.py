from http import client
from dataclasses import dataclass


@dataclass
class HttpRequest:
    host: str
    method: str
    uri: str
    headers: client.HTTPMessage
    body: bytes
