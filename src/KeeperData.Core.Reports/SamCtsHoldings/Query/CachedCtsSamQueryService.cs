using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.SamCtsHoldings.Query;

[ExcludeFromCodeCoverage(Justification = "Analysis context with query service dependency - covered by integration tests.")]
public sealed class CachedCtsSamQueryService(ICtsSamQueryService dataService) : ICtsSamQueryService
{
    private readonly ConcurrentDictionary<string, Lazy<Task<object?>>> _cache = new();

    public Task<CtsCphHoldingModel?> GetCtsCphHoldingAsync(LidFullIdentifier lidFullIdentifier, CancellationToken ct)
        => GetOrAddAsync($"cts-holding-lid:{lidFullIdentifier.Value}",
            () => dataService.GetCtsCphHoldingAsync(lidFullIdentifier, ct));

    public Task<CtsCphHoldingModel?> GetCtsCphHoldingAsync(Cph cph, CancellationToken ct)
        => GetOrAddAsync($"cts-holding-cph:{cph.Value}",
            () => dataService.GetCtsCphHoldingAsync(cph, ct));

    public Task<int> GetCtsCphHoldingsCountAsync(CancellationToken ct)
        => GetOrAddAsync("cts-holdings-count",
            () => dataService.GetCtsCphHoldingsCountAsync(ct));

    public Task<SamCphHoldingModel?> GetSamCphHoldingAsync(Cph cph, CancellationToken ct)
        => GetOrAddAsync($"sam-holding-cph:{cph.Value}",
            () => dataService.GetSamCphHoldingAsync(cph, ct));

    public Task<int> GetSamCphHoldingsCountAsync(CancellationToken ct)
        => GetOrAddAsync("sam-holdings-count",
            () => dataService.GetSamCphHoldingsCountAsync(ct));

    public Task<QueryResult> ListCtsCphHoldingsAsync(int skip, int take, CancellationToken ct)
        => GetOrAddAsync($"cts-holdings:{skip}:{take}",
            () => dataService.ListCtsCphHoldingsAsync(skip, take, ct));

    public Task<QueryResult> ListSamCphHoldingsAsync(int skip, int take, CancellationToken ct)
        => GetOrAddAsync($"sam-holdings:{skip}:{take}",
            () => dataService.ListSamCphHoldingsAsync(skip, take, ct));

    private async Task<T> GetOrAddAsync<T>(string cacheKey, Func<Task<T>> factory)
    {
        var lazyTask = _cache.GetOrAdd(cacheKey, _ => new Lazy<Task<object?>>(
            async () => (object?)await factory(),
            LazyThreadSafetyMode.ExecutionAndPublication));

        return (T)(await lazyTask.Value)!;
    }
}