using System.Text.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.SystemTextJson;

[assembly: LambdaSerializer(typeof(DefaultLambdaJsonSerializer))]

namespace LmiTest;

public class Function
{
    public Task<object> FunctionHandler(object _event, ILambdaContext context)
    {
        var body = JsonSerializer.Serialize(new
        {
            message = "hello world",
            runtime = "dotnet8",
            requestId = context.AwsRequestId,
        });

        return Task.FromResult<object>(new
        {
            statusCode = 200,
            body,
        });
    }
}
