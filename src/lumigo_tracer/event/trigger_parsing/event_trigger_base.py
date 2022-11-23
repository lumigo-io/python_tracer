import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, List, Dict, Any, Union


class ExtraKeys(str, Enum):
    RECORDS_NUM = "recordsNum"
    ARN = "arn"
    RESOURCE = "resource"
    HTTP_METHOD = "httpMethod"
    API = "api"
    STAGE = "stage"
    REGION = "region"
    DETAIL_TYPE = "detailType"
    TRIGGER_CREATION_TIME = "approxEventCreationTime"
    TOTAL_SIZE = "totalSizeBytes"
    SHARD_ID = "shardId"


ExtraType = Dict[ExtraKeys, Union[str, int, None]]
TriggerType = Dict[str, Union[str, List[str], ExtraType, None]]


class EventTriggerParser(ABC):
    # We boost our performance using quick scan of magic-words on internal messages (chained triggers).
    # If the service of this parser can be chained, this value should be overridden.
    MAGIC_IDENTIFIER: Optional[str] = None

    @staticmethod
    def build_trigger(
        target_id: Optional[str],
        resource_type: str,
        from_message_ids: Optional[List[str]] = None,
        extra: Optional[ExtraType] = None,
    ) -> TriggerType:
        result: TriggerType = {
            "id": str(uuid.uuid4()),
            "targetId": target_id,
            "triggeredBy": resource_type,
        }
        if extra:
            result["extra"] = extra
        if from_message_ids:
            result["fromMessageIds"] = from_message_ids
        return result

    @classmethod
    def should_handle(cls, event: Dict[Any, Any]) -> bool:
        try:
            return cls._should_handle(event)
        except Exception:
            return False

    @staticmethod
    @abstractmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        raise NotImplementedError()

    @staticmethod
    def extract_inner(event: Dict[Any, Any]) -> List[str]:
        return []
