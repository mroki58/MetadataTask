using System.Text.Json;
using FivetranClient.Dtos;

namespace FivetranClient.Fetchers;

public sealed class NonPaginatedFetcher(HttpRequestHandler requestHandler) : BaseFetcher(requestHandler)
{
    public async Task<T?> FetchAsync<T>(string endpoint, CancellationToken cancellationToken)
    {
        var root = await GetAndDeserializeAsync<NonPaginatedRoot<T>>(endpoint, cancellationToken);
        return root is null ? default(T) : root.Data;
    }
}