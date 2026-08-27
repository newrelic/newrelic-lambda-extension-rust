require 'json'
require 'aws-sdk-s3'
require 'aws-sdk-dynamodb'
require 'aws-sdk-lambda'

S3_CLIENT  = Aws::S3::Client.new
DDB_CLIENT = Aws::DynamoDB::Client.new
LAMBDA_CLIENT = Aws::Lambda::Client.new

BUCKET  = ENV['TEST_BUCKET_NAME']
TABLE   = ENV['TEST_TABLE_NAME']
ECHO_FN = ENV['ECHO_FUNCTION_NAME']

def lambda_handler(event:, context:)
  req_id = context.aws_request_id

  S3_CLIENT.put_object(
    bucket: BUCKET,
    key: "ruby/#{req_id}.json",
    body: JSON.generate({ runtime: 'ruby3.3', requestId: req_id }),
    content_type: 'application/json'
  )

  DDB_CLIENT.put_item(
    table_name: TABLE,
    item: {
      'requestId' => req_id,
      'runtime'   => 'ruby3.3',
      'event'     => JSON.generate(event)[0, 1024]
    }
  )

  echo_resp = LAMBDA_CLIENT.invoke(
    function_name: ECHO_FN,
    invocation_type: 'RequestResponse',
    payload: JSON.generate({ source: 'ruby', requestId: req_id })
  )
  echo_payload = JSON.parse(echo_resp.payload.read)

  {
    statusCode: 200,
    body: JSON.generate({
      runtime: 'ruby3.3',
      requestId: req_id,
      s3Key: "ruby/#{req_id}.json",
      echoResponse: echo_payload
    })
  }
end
