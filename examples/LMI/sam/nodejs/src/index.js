module.exports.handler = async (_event, context) => {
  return {
    statusCode: 200,
    body: JSON.stringify({
      message: "hello world",
      runtime: "nodejs22",
      requestId: context.awsRequestId,
    }),
  };
};
