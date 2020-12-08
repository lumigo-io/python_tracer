from typing import Optional

from lumigo_tracer.logger import get_logger

try:
    import botocore
except Exception:
    get_logger().warning("botocore is missing")
    botocore = None

try:
    import boto3
except Exception:
    get_logger().warning("boto3 is missing")
    boto3 = None


def get_boto_client(
    service: str,
    region: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
):
    if not boto3:
        return None
    return boto3.client(
        service,
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
