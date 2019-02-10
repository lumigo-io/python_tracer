import json

from lumigo_tracer.async_http.async_hook import lumigo_async_lambda
import aioboto3
import aiohttp
import asyncio


@lumigo_async_lambda
async def my_lambda():
    await aioboto3.resource("dynamodb", region_name="us-east-2").Table("test").put_item(
        Item={"a": 1, "key": 2}
    )

    await aioboto3.client("sns").publish(
        TargetArn="arn:aws:sns:us-east-2:723663554526:test", Message=json.dumps({"test": "test"})
    )

    google = await aiohttp.ClientSession().get("http://www.google.com")
    await google.text()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(my_lambda())
