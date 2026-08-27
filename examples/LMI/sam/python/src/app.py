import json


def lambda_handler(_event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            "runtime": "python3.12",
            "requestId": context.aws_request_id,
        }),
    }
