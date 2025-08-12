using System.Net;
using System.Runtime.CompilerServices;
using FivetranClient.Dtos;

namespace FivetranClient.Fetchers;

public sealed class PaginatedFetcher(HttpRequestHandler requestHandler) : BaseFetcher(requestHandler)
{
    private const ushort PageSize = 100;

    private static string BuildPaginatedEndpoint(string endpoint, int pageSize, string? cursor = null)
    {
        var url = $"{endpoint}?limit={pageSize}";
        if (!string.IsNullOrWhiteSpace(cursor))
        {
            url += $"&cursor={WebUtility.UrlEncode(cursor)}";
        }
        return url;
    }

    public IAsyncEnumerable<T> FetchItemsAsync<T>(string endpoint, CancellationToken cancellationToken)
    {
        var firstPageTask = FetchPageAsync<T>(endpoint, cancellationToken);
        return ProcessPagesRecursivelyAsync(endpoint, firstPageTask, cancellationToken);
    }

    private Task<PaginatedRoot<T>?> FetchPageAsync<T>(
        string endpoint,
        CancellationToken cancellationToken,
        string? cursor = null)
    {
        var fullEndpoint = BuildPaginatedEndpoint(endpoint, PageSize, cursor);
        return GetAndDeserializeAsync<PaginatedRoot<T>>(fullEndpoint, cancellationToken);
    }

    private async IAsyncEnumerable<T> ProcessPagesRecursivelyAsync<T>(
        string endpoint,
        Task<PaginatedRoot<T>?> currentPageTask,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var currentPage = await currentPageTask;
        var nextCursor = currentPage?.Data?.NextCursor;

        IAsyncEnumerable<T>? nextResults = null;
        if (!string.IsNullOrWhiteSpace(nextCursor))
        {
            var nextTask = FetchPageAsync<T>(endpoint, cancellationToken, nextCursor);
            nextResults = ProcessPagesRecursivelyAsync(endpoint, nextTask, cancellationToken);
        }

        foreach (var item in currentPage?.Data?.Items ?? [])
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }

        if (nextResults is not null)
        {
            await foreach (var nextItem in nextResults.WithCancellation(cancellationToken))
            {
                yield return nextItem;
            }
        }
    }
}