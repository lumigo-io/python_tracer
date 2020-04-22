import json
from collections import OrderedDict

from lumigo_tracer.parsers.event_parser import parse_event


def test_parse_event_not_api_gw():
    event = {"a": 1}

    new_event = parse_event(event=event)

    assert new_event == event


def test_parse_event_api_gw():
    not_order_api_gw_event = {
        "resource": "/add-user",
        "path": "/add-user",
        "httpMethod": "POST",
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
            "Authorization": "eyJraWQiOiIrbG90QWhYczBhQWFxRnI0Q0MwalVnTGVHRGRKQ2NYRkJOSHNkUFRcL0Jucz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhODcwMDViYi0zMDMwLTQ5NjItYmFlOC00OGNkNjI5YmEyMGIiLCJjdXN0b206Y3VzdG9tZXIiOiJjX2I0ODZlZTVhMDk3MTQiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtd2VzdC0yLmFtYXpvbmF3cy5jb21cL3VzLXdlc3QtMl8yckVBOVp0aDYiLCJjdXN0b206Y3VzdG9tZXItbmFtZSI6IldhbHR5IiwiY29nbml0bzp1c2VybmFtZSI6ImE4NzAwNWJiLTMwMzAtNDk2Mi1iYWU4LTQ4Y2Q2MjliYTIwYiIsImF1ZCI6IjRsaWRjbmVrNTBoaTE4OTk2Z2FkYW9wOGowIiwiZXZlbnRfaWQiOiI5ZmU4MDczNS1mMjY1LTQxZDUtYTdjYS0wNGI4OGMyYTRhNGMiLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTU4NzAzODc0NCwiZXhwIjoxNTg3MjgzNTc0LCJjdXN0b206cm9sZSI6ImFkbWluIiwiaWF0IjoxNTg3Mjc5OTc0LCJlbWFpbCI6ImRvckB3YWx0eS5jby5pbCJ9.BWDTwhSNIOrKpoeEsdKAJ__CU72O7d_4LtDYBgPLIqvZJXQanAg4LGXXf00aivx0R_rFyZxChZjbzU4UGqbNDU7QpMH8QWWdrjW3oP8SGVH_C62PHO_7NA0iXM3PM6LH1IcmkjDcZ31lprIQ7B9l26lyW5x_VfDvEecE-VjeauYnFjCq1-hOFzn9UDo2rPTn6mg6FE8KMGQdqcXM0HcJhP2NrvnDI5J3_Xh1qai_VtzG70dVISCJ1zMesTtzrpCvRCTJwcEMLVsdbYK4VoK1U9E4SksdGOOc6_8nsMyasKbzeueOsN29YV1_7Oz9BqJeW_7WFZ5UiNc6XNNTnwNj-w",
            "CloudFront-Forwarded-Proto": "https",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-Mobile-Viewer": "false",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Tablet-Viewer": "false",
            "CloudFront-Viewer-Country": "IL",
            "content-type": "application/json;charset=UTF-8",
            "customer_id": "c_b486ee5a09714",
            "Host": "psqn7b0ev2.execute-api.us-west-2.amazonaws.com",
            "origin": "https://platform.lumigo.io",
            "Referer": "https://platform.lumigo.io/users",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36",
            "Via": "2.0 59574f77a7cf2d23d64904db278e5711.cloudfront.net (CloudFront)",
            "X-Amz-Cf-Id": "J4KbOEUrZCnUQSLsDq1PyYXmfpVy8x634huSeBX0HCbscgH-N2AtVA==",
            "X-Amzn-Trace-Id": "Root=1-5e9bf868-1c53a38cfe070266db0bfbd9",
            "X-Forwarded-For": "5.102.206.161, 54.182.243.106",
            "X-Forwarded-Port": "443",
            "X-Forwarded-Proto": "https",
        },
        "multiValueHeaders": {
            "Accept": ["application/json, text/plain, */*"],
            "Accept-Encoding": ["gzip, deflate, br"],
            "Accept-Language": ["he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7"],
            "Authorization": [
                "eyJraWQiOiIrbG90QWhYczBhQWFxRnI0Q0MwalVnTGVHRGRKQ2NYRkJOSHNkUFRcL0Jucz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhODcwMDViYi0zMDMwLTQ5NjItYmFlOC00OGNkNjI5YmEyMGIiLCJjdXN0b206Y3VzdG9tZXIiOiJjX2I0ODZlZTVhMDk3MTQiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtd2VzdC0yLmFtYXpvbmF3cy5jb21cL3VzLXdlc3QtMl8yckVBOVp0aDYiLCJjdXN0b206Y3VzdG9tZXItbmFtZSI6IldhbHR5IiwiY29nbml0bzp1c2VybmFtZSI6ImE4NzAwNWJiLTMwMzAtNDk2Mi1iYWU4LTQ4Y2Q2MjliYTIwYiIsImF1ZCI6IjRsaWRjbmVrNTBoaTE4OTk2Z2FkYW9wOGowIiwiZXZlbnRfaWQiOiI5ZmU4MDczNS1mMjY1LTQxZDUtYTdjYS0wNGI4OGMyYTRhNGMiLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTU4NzAzODc0NCwiZXhwIjoxNTg3MjgzNTc0LCJjdXN0b206cm9sZSI6ImFkbWluIiwiaWF0IjoxNTg3Mjc5OTc0LCJlbWFpbCI6ImRvckB3YWx0eS5jby5pbCJ9.BWDTwhSNIOrKpoeEsdKAJ__CU72O7d_4LtDYBgPLIqvZJXQanAg4LGXXf00aivx0R_rFyZxChZjbzU4UGqbNDU7QpMH8QWWdrjW3oP8SGVH_C62PHO_7NA0iXM3PM6LH1IcmkjDcZ31lprIQ7B9l26lyW5x_VfDvEecE-VjeauYnFjCq1-hOFzn9UDo2rPTn6mg6FE8KMGQdqcXM0HcJhP2NrvnDI5J3_Xh1qai_VtzG70dVISCJ1zMesTtzrpCvRCTJwcEMLVsdbYK4VoK1U9E4SksdGOOc6_8nsMyasKbzeueOsN29YV1_7Oz9BqJeW_7WFZ5UiNc6XNNTnwNj-w"
            ],
            "CloudFront-Forwarded-Proto": ["https"],
            "CloudFront-Is-Desktop-Viewer": ["true"],
            "CloudFront-Is-Mobile-Viewer": ["false"],
            "CloudFront-Is-SmartTV-Viewer": ["false"],
            "CloudFront-Is-Tablet-Viewer": ["false"],
            "CloudFront-Viewer-Country": ["IL"],
            "content-type": ["application/json;charset=UTF-8"],
            "customer_id": ["c_b486ee5a09714"],
            "Host": ["psqn7b0ev2.execute-api.us-west-2.amazonaws.com"],
            "origin": ["https://platform.lumigo.io"],
            "Referer": ["https://platform.lumigo.io/users"],
            "sec-fetch-dest": ["empty"],
            "sec-fetch-mode": ["cors"],
            "sec-fetch-site": ["cross-site"],
            "User-Agent": [
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
            ],
            "Via": ["2.0 59574f77a7cf2d23d64904db278e5711.cloudfront.net (CloudFront)"],
            "X-Amz-Cf-Id": ["J4KbOEUrZCnUQSLsDq1PyYXmfpVy8x634huSeBX0HCbscgH-N2AtVA=="],
            "X-Amzn-Trace-Id": ["Root=1-5e9bf868-1c53a38cfe070266db0bfbd9"],
            "X-Forwarded-For": ["5.102.206.161, 54.182.243.106"],
            "X-Forwarded-Port": ["443"],
            "X-Forwarded-Proto": ["https"],
        },
        "queryStringParameters": "1",
        "multiValueQueryStringParameters": "1",
        "pathParameters": "1",
        "stageVariables": None,
        "requestContext": {
            "resourceId": "ua33sn",
            "authorizer": {
                "claims": {
                    "sub": "a87005bb-3030-4962-bae8-48cd629ba20b",
                    "custom:customer": "c_b486ee5a09714",
                    "iss": "https://cognito-idp.us-west-2.amazonaws.com/us-west-2_2rEA9Zth6",
                    "custom:customer-name": "Walty",
                    "cognito:username": "a87005bb-3030-4962-bae8-48cd629ba20b",
                    "aud": "4lidcnek50hi18996gadaop8j0",
                    "event_id": "9fe80735-f265-41d5-a7ca-04b88c2a4a4c",
                    "token_use": "id",
                    "auth_time": "1587038744",
                    "exp": "Sun Apr 19 08:06:14 UTC 2020",
                    "custom:role": "admin",
                    "iat": "Sun Apr 19 07:06:14 UTC 2020",
                    "email": "dor@walty.co.il",
                }
            },
            "resourcePath": "/add-user",
            "httpMethod": "POST",
            "extendedRequestId": "LOPAXFcuvHcFUKg=",
            "requestTime": "19/Apr/2020:07:06:16 +0000",
            "path": "/prod/add-user",
            "accountId": "114300393969",
            "protocol": "HTTP/1.1",
            "stage": "prod",
            "domainPrefix": "psqn7b0ev2",
            "requestTimeEpoch": 1587279976628,
            "requestId": "78542821-ca17-4e83-94ec-96993a9d451d",
            "identity": {
                "cognitoIdentityPoolId": None,
                "accountId": None,
                "cognitoIdentityId": None,
                "caller": None,
                "sourceIp": "5.102.206.161",
                "principalOrgId": None,
                "accessKey": None,
                "cognitoAuthenticationType": None,
                "cognitoAuthenticationProvider": None,
                "userArn": None,
                "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36",
                "user": None,
            },
            "domainName": "psqn7b0ev2.execute-api.us-west-2.amazonaws.com",
            "apiId": "psqn7b0ev2",
        },
        "body": '{"email":"orrduer@gmail.com"}',
        "isBase64Encoded": False,
    }

    order_api_gw_event = parse_event(event=not_order_api_gw_event)

    assert json.dumps(order_api_gw_event) == json.dumps(
        OrderedDict(
            {
                "resource": "/add-user",
                "path": "/add-user",
                "httpMethod": "POST",
                "queryStringParameters": "1",
                "multiValueQueryStringParameters": "1",
                "pathParameters": "1",
                "body": '{"email":"orrduer@gmail.com"}',
                "requestContext": {
                    "authorizer": {
                        "claims": {
                            "sub": "a87005bb-3030-4962-bae8-48cd629ba20b",
                            "custom:customer": "c_b486ee5a09714",
                            "iss": "https://cognito-idp.us-west-2.amazonaws.com/us-west-2_2rEA9Zth6",
                            "custom:customer-name": "Walty",
                            "cognito:username": "a87005bb-3030-4962-bae8-48cd629ba20b",
                            "aud": "4lidcnek50hi18996gadaop8j0",
                            "event_id": "9fe80735-f265-41d5-a7ca-04b88c2a4a4c",
                            "token_use": "id",
                            "auth_time": "1587038744",
                            "exp": "Sun Apr 19 08:06:14 UTC 2020",
                            "custom:role": "admin",
                            "iat": "Sun Apr 19 07:06:14 UTC 2020",
                            "email": "dor@walty.co.il",
                        }
                    }
                },
                "headers": {
                    "Authorization": "eyJraWQiOiIrbG90QWhYczBhQWFxRnI0Q0MwalVnTGVHRGRKQ2NYRkJOSHNkUFRcL0Jucz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhODcwMDViYi0zMDMwLTQ5NjItYmFlOC00OGNkNjI5YmEyMGIiLCJjdXN0b206Y3VzdG9tZXIiOiJjX2I0ODZlZTVhMDk3MTQiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtd2VzdC0yLmFtYXpvbmF3cy5jb21cL3VzLXdlc3QtMl8yckVBOVp0aDYiLCJjdXN0b206Y3VzdG9tZXItbmFtZSI6IldhbHR5IiwiY29nbml0bzp1c2VybmFtZSI6ImE4NzAwNWJiLTMwMzAtNDk2Mi1iYWU4LTQ4Y2Q2MjliYTIwYiIsImF1ZCI6IjRsaWRjbmVrNTBoaTE4OTk2Z2FkYW9wOGowIiwiZXZlbnRfaWQiOiI5ZmU4MDczNS1mMjY1LTQxZDUtYTdjYS0wNGI4OGMyYTRhNGMiLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTU4NzAzODc0NCwiZXhwIjoxNTg3MjgzNTc0LCJjdXN0b206cm9sZSI6ImFkbWluIiwiaWF0IjoxNTg3Mjc5OTc0LCJlbWFpbCI6ImRvckB3YWx0eS5jby5pbCJ9.BWDTwhSNIOrKpoeEsdKAJ__CU72O7d_4LtDYBgPLIqvZJXQanAg4LGXXf00aivx0R_rFyZxChZjbzU4UGqbNDU7QpMH8QWWdrjW3oP8SGVH_C62PHO_7NA0iXM3PM6LH1IcmkjDcZ31lprIQ7B9l26lyW5x_VfDvEecE-VjeauYnFjCq1-hOFzn9UDo2rPTn6mg6FE8KMGQdqcXM0HcJhP2NrvnDI5J3_Xh1qai_VtzG70dVISCJ1zMesTtzrpCvRCTJwcEMLVsdbYK4VoK1U9E4SksdGOOc6_8nsMyasKbzeueOsN29YV1_7Oz9BqJeW_7WFZ5UiNc6XNNTnwNj-w",
                    "content-type": "application/json;charset=UTF-8",
                    "customer_id": "c_b486ee5a09714",
                    "Host": "psqn7b0ev2.execute-api.us-west-2.amazonaws.com",
                    "origin": "https://platform.lumigo.io",
                    "Referer": "https://platform.lumigo.io/users",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36",
                },
                "stageVariables": None,
                "isBase64Encoded": False,
            }
        )
    )
