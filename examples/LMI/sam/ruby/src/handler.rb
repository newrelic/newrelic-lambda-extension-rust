require 'json'

def lambda_handler(event:, context:)
  {
    statusCode: 200,
    body: JSON.generate({
      message: 'hello world',
      runtime: 'ruby3.3',
      requestId: context.aws_request_id,
      receivedEvent: event
    })
  }
end
