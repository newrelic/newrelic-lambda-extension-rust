"""
Lambda handler for RIE integration testing.

Invocation event schema:
  {
    "log_count":  100,     # how many logs to emit (default 100)
    "log_prefix": "inv-1"  # prefix for each log line  (default "test")
  }

Logs are emitted as plain numbered lines so they are trivially visible
in CloudWatch / mock-server output and easy to count for missed-log checks:

    [inv-1] log 001
    [inv-1] log 002
    ...
    [inv-1] log 100
"""

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    log_count = int(event.get("log_count", 100))
    prefix = event.get("log_prefix", "test")
    request_id = context.aws_request_id

    for i in range(1, log_count + 1):
        logger.info("[%s] log %03d  req=%s", prefix, i, request_id[:8])

    return {
        "statusCode": 200,
        "body": f'{{"logs_emitted":{log_count},"prefix":"{prefix}","request_id":"{request_id}"}}',
    }
