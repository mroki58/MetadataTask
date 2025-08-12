using System.Text.Json;

namespace FivetranClient.Fetchers;

public abstract class BaseFetcher(HttpRequestHandler requestHandler)
{
    protected readonly HttpRequestHandler RequestHandler = requestHandler;
    protected static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true,
    };

    protected async Task<T?> GetAndDeserializeAsync<T>(string endpoint, CancellationToken cancellationToken)
    {
        var content = await RequestHandler.GetAsync(endpoint, cancellationToken);
        return JsonSerializer.Deserialize<T>(content, SerializerOptions);
    }
}