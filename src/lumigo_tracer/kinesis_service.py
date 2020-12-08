import random
import json
from collections import Counter
from typing import Callable, Any, List, Dict, Tuple, Optional

from lumigo_tracer import boto
from lumigo_tracer.logger import get_logger


MAX_ITEM_SIZE = 1_048_576
ENCODED_64_COST = 4 / 3
MAX_KINESIS_BATCH_SIZE = 250
ALLOW_RETRY_ERROR_CODES = [
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "ServiceUnavailable",
    "ProvisionedThroughputExceededException",
    "RequestExpired",
]


class KinesisService:
    """
    KinesisService client to send a batch of records to kinesis and log errors if any occurred.
    :param stream_name: Kinesis stream name to send to.
    :param region: The region in which the kinesis is found.
    :param max_batch_size: The max number of records in batch (AWS limit is 500)
    :param partition_key_function: this function get the event and return the partition
    """

    def __init__(
        self,
        stream_name: str,
        region: str,
        max_batch_size: int = MAX_KINESIS_BATCH_SIZE,
        partition_key_function: Callable[[Any], str] = lambda _: str(random.random()),
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        self.stream_name = stream_name
        self.region = region
        self.partition_key_function = partition_key_function
        self.max_batch_size = max_batch_size
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        if self.max_batch_size > MAX_KINESIS_BATCH_SIZE:
            get_logger().warning(
                f"Max kinesis batch size can't be more than MAX_KINESIS_BATCH_SIZE, got: {max_batch_size}"
            )
            self.max_batch_size = MAX_KINESIS_BATCH_SIZE

    def _get_stream_client(self) -> Any:
        return boto.get_boto_client(
            service="kinesis",
            region=self.region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def _create_kinesis_event(self, raw_event: Any) -> Dict:
        return {
            "Data": raw_event if isinstance(raw_event, bytes) else json.dumps(raw_event),
            "PartitionKey": self.partition_key_function(raw_event),
        }

    def send(self, events: List[Any]) -> Optional[Dict]:
        client_kinesis = self._get_stream_client()
        if not client_kinesis:
            get_logger().error("Unable create a Kinesis client. Unable to send.")
            return None
        response_records: List = []
        records_to_write: List = []
        retry_items_len = 0
        raw_events = list(map(self._create_kinesis_event, events.copy()))
        while len(raw_events) > 0:
            event = raw_events.pop(0)
            if isinstance(event.get("Data"), bytes):
                event_size = len(event["Data"])
            else:
                event_size = len(json.dumps(event))
            # Understand the encoding 64 cost
            get_logger().debug(
                f"Event before sending to Kinesis. Size: {event_size * ENCODED_64_COST}. Type: {type(event.get('Data'))}"
            )
            if event_size > MAX_ITEM_SIZE:
                get_logger().error(f"Event is too big, skipping... Size: {event_size}")
                continue
            records_to_write.append(event)
            if len(raw_events) == 0 or len(records_to_write) == self.max_batch_size:
                number_of_records_to_send = len(records_to_write)
                try:
                    kinesis_response = client_kinesis.put_records(
                        Records=records_to_write, StreamName=self.stream_name
                    )
                    retry_items, bad_items = KinesisService._parse_kinesis_results_to_lists(
                        kinesis_response
                    )
                    response_records.extend(kinesis_response.get("Records", []))
                    self._log_iteration(retry_items, bad_items, number_of_records_to_send)
                    retry_items_len += len(retry_items)

                except Exception as err:
                    events_log = str(records_to_write)
                    log_extra = {
                        "stream_name": self.stream_name,
                        "stream_region": self.region,
                        "records_to_write": events_log,
                        "error": str(err),
                    }
                    get_logger().error(f"Unexpected error in send to kinesis. {log_extra}")
                records_to_write.clear()
        self._log_end(response_records, len(events))
        return {"records": response_records, "retried_items_count": retry_items_len}

    @staticmethod
    def _parse_kinesis_results_to_lists(kinesis_result: dict) -> Tuple[list, list]:
        parsed_items = [
            (index, item.get("ErrorCode"))
            for (index, item) in enumerate(kinesis_result.get("Records", []))
            if item.get("ErrorCode")
        ]
        retry_items = [item for item in parsed_items if item[1] in ALLOW_RETRY_ERROR_CODES]
        bad_items = [item for item in parsed_items if item[1] not in ALLOW_RETRY_ERROR_CODES]
        return retry_items, bad_items

    @staticmethod
    def _sum_items_by_error_codes(items_list) -> Counter:
        items_list_sum: Counter = Counter()
        for (index, error_code) in items_list:
            items_list_sum += Counter({error_code: 1})
        return items_list_sum

    def _log_end(self, response_records: list, raw_events_len: int):
        total_pushed = [record for record in response_records if record.get("ShardId")]
        is_missing_events = (raw_events_len - len(total_pushed)) > 0
        log_extra = {
            "pushed_items": len(total_pushed),
            "raw_events_len": raw_events_len,
            "total_retries": len(response_records) - len(total_pushed),
            "stream_name": self.stream_name,
            "missing_events": raw_events_len - len(total_pushed),
            "is_missing_events": is_missing_events,
        }
        if is_missing_events:
            get_logger().warning(f"Push to kinesis partially successful. {log_extra}")
        else:
            get_logger().info(f"Push to kinesis successfully done. {log_extra}")

    def _log_iteration(self, retry_items, bad_items, total_items_len):
        failed_len = len(retry_items) + len(bad_items)
        success_len = total_items_len - failed_len
        retry_items_sum = KinesisService._sum_items_by_error_codes(retry_items)
        bad_items_sum = KinesisService._sum_items_by_error_codes(bad_items)
        log_extra = {
            "stream_name": self.stream_name,
            "stream_region": self.region,
            "number_of_failed_kinesis_records": failed_len,
            "failed_kinesis_records_by_error_code": bad_items_sum,
            "retried_kinesis_records_by_error_code": retry_items_sum,
            "number_of_success_kinesis_records": success_len,
        }
        get_logger().info(f"There were kinesis records writes. {log_extra}")
