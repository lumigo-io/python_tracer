from lumigo_tracer import lumigo_tracer


@lumigo_tracer(
    edge_host="https://336baui8uh.execute-api.us-west-2.amazonaws.com/api/spans",
    token="t_71cb3d565019d1993b3c",
)
def handler(event, context):
    print(event)
    return {"statusCode": 200, "headers": {}, "body": "Hello Lumigo!"}
