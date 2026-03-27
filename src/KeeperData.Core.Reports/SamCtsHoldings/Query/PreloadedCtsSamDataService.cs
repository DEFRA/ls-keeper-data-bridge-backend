using System.Diagnostics;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Operations;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using KeeperData.Core.Throttling;
using Microsoft.Extensions.Logging;
using static KeeperData.Core.Reports.SamCtsHoldings.Query.Domain.DataFields;

namespace KeeperData.Core.Reports.SamCtsHoldings.Query;

public sealed class PreloadedCtsSamDataService(
    DataSetDefinitions dataSetDefinitions,
    IQueryService queryService,
    IThrottler throttler,
    ILogger<PreloadedCtsSamDataService> logger) : IPreloadedCtsSamDataService
{
    // CTS lookups
    private readonly List<Dictionary<string, object?>> _ctsCphHoldings = [];
    private readonly Dictionary<string, Dictionary<string, object?>> _ctsCphHoldingsByLid = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Dictionary<string, object?>> _ctsCphHoldingsByCph = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<Dictionary<string, object?>>> _ctsKeepersByLid = new(StringComparer.OrdinalIgnoreCase);

    // SAM lookups
    private readonly List<Dictionary<string, object?>> _samCphHoldings = [];
    private readonly Dictionary<string, Dictionary<string, object?>> _samCphHoldingsByCph = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<Dictionary<string, object?>>> _samHerdsByCph = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<Dictionary<string, object?>>> _samPartiesByPartyId = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<Dictionary<string, object?>>> _samCphHoldersByCph = new(StringComparer.OrdinalIgnoreCase);

    private bool _loaded;
    private int _totalPreloadedRecords;

    public async Task PreloadAsync(CancellationToken ct, OperationScope? scope = null, Func<bool>? isCancellationRequested = null)
    {
        if (_loaded)
            throw new InvalidOperationException("PreloadAsync has already been called. This service instance cannot be preloaded more than once.");
        _loaded = true;

        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | PreloadAsync | BEGIN");
        var sw = Stopwatch.StartNew();

        // ── Count all collections upfront so we can report % complete ────────
        var collectionNames = new[]
        {
            dataSetDefinitions.CTSCPHHolding.Name,
            dataSetDefinitions.CTSKeeper.Name,
            dataSetDefinitions.SamCPHHolding.Name,
            dataSetDefinitions.SamHerd.Name,
            dataSetDefinitions.SamParty.Name,
            dataSetDefinitions.SamCPHHolder.Name
        };
        var (counts, countMs) = await Timed.RunAsync(async () =>
        {
            var countTasks = collectionNames.Select(name => CountCollectionAsync(name, ct)).ToArray();
            await Task.WhenAll(countTasks);
            return countTasks.Select(t => t.Result).ToArray();
        });
        scope?.TrackElapsed("counting", countMs);

        var totalRecords = counts.Sum();
        _totalPreloadedRecords = (int)totalRecords;
        scope?.Start((int)totalRecords, $"Loading {totalRecords:N0} records from {collectionNames.Length} collections");
        Trace.TraceInformation($"KRDSBRIDGE | PreloadAsync | Counts retrieved: {string.Join(", ", collectionNames.Zip(counts, (n, c) => $"{n}={c}"))} total={totalRecords}, countDuration={countMs}ms");

        // Create per-collection child scopes (Start() is deferred to each load method)
        var collectionScopes = collectionNames.Select(name => scope?.CreateChild(name)).ToArray();

        // CTS and SAM collections are independent — load them in parallel
        await Task.WhenAll(
            LoadCtsGroupAsync(ct, collectionScopes[0], (int)counts[0], collectionScopes[1], (int)counts[1], isCancellationRequested),
            LoadSamGroupAsync(ct,
                (collectionScopes[2], (int)counts[2]),
                (collectionScopes[3], (int)counts[3]),
                (collectionScopes[4], (int)counts[4]),
                isCancellationRequested));

        // Holders depend on both CTS + SAM CPH keys being populated
        await LoadSamCphHoldersAsync(ct, collectionScopes[5], (int)counts[5], isCancellationRequested);

        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | PreloadAsync | END, duration={sw.ElapsedMilliseconds}ms");
        logger.LogInformation(
            "PreloadAsync completed. CTS Holdings={CtsHoldings}, CTS Keepers={CtsKeepers}, SAM Holdings={SamHoldings}, SAM Herds={SamHerds}, SAM Parties={SamParties}, SAM Holders={SamHolders}, Duration={DurationMs}ms",
            _ctsCphHoldings.Count,
            _ctsKeepersByLid.Values.Sum(v => v.Count),
            _samCphHoldings.Count,
            _samHerdsByCph.Values.Sum(v => v.Count),
            _samPartiesByPartyId.Values.Sum(v => v.Count),
            _samCphHoldersByCph.Values.Sum(v => v.Count),
            sw.ElapsedMilliseconds);
    }

    public CtsCphHoldingModel? GetCtsCphHolding(LidFullIdentifier lidFullIdentifier)
    {
        if (!_ctsCphHoldingsByLid.TryGetValue(lidFullIdentifier.Value, out var holding))
            return null;

        var keepers = _ctsKeepersByLid.GetValueOrDefault(lidFullIdentifier.Value) ?? [];
        return new CtsCphHoldingModel
        {
            Id = lidFullIdentifier,
            Holding = holding,
            Keepers = BuildQueryResult(dataSetDefinitions.CTSKeeper.Name, keepers)
        };
    }

    public CtsCphHoldingModel? GetCtsCphHolding(Cph cph)
    {
        if (!_ctsCphHoldingsByCph.TryGetValue(cph.Value, out var holding))
            return null;

        var lid = LidFullIdentifier.TryParse(holding[CtsCphHoldingFields.LidFullIdentifier]?.ToString());
        if (lid is null)
            return null;

        var keepers = _ctsKeepersByLid.GetValueOrDefault(lid.Value) ?? [];
        return new CtsCphHoldingModel
        {
            Id = lid,
            Holding = holding,
            Keepers = BuildQueryResult(dataSetDefinitions.CTSKeeper.Name, keepers)
        };
    }

    public int GetCtsCphHoldingsCount() => _ctsCphHoldings.Count;

    public int GetTotalPreloadedRecordCount() => _totalPreloadedRecords;

    public SamCphHoldingModel? GetSamCphHolding(Cph cph)
    {
        if (!_samCphHoldingsByCph.TryGetValue(cph.Value, out var holding))
            return null;

        var herds = _samHerdsByCph.GetValueOrDefault(cph.Value) ?? [];

        var partyIds = herds
            .SelectMany(x => new[]
            {
                x[SamHerd.OwnerPartyIds]?.ToString(),
                x[SamHerd.KeeperPartyIds]?.ToString()
            })
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .SelectMany(s => s!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        var parties = partyIds
            .SelectMany(id => _samPartiesByPartyId.GetValueOrDefault(id) ?? [])
            .ToList();

        var holders = _samCphHoldersByCph.GetValueOrDefault(cph.Value) ?? [];

        return new SamCphHoldingModel
        {
            Cph = cph,
            Holding = holding,
            Herd = BuildQueryResult(dataSetDefinitions.SamHerd.Name, herds),
            Parties = BuildQueryResult(dataSetDefinitions.SamParty.Name, parties),
            Holders = BuildQueryResult(dataSetDefinitions.SamCPHHolder.Name, holders)
        };
    }

    public int GetSamCphHoldingsCount() => _samCphHoldings.Count;

    public QueryResult ListCtsCphHoldings(int skip, int take)
    {
        var count = Math.Min(take, _ctsCphHoldings.Count - skip);
        var page = count > 0 ? _ctsCphHoldings.GetRange(skip, count) : [];
        return new QueryResult
        {
            CollectionName = dataSetDefinitions.CTSCPHHolding.Name,
            Data = page,
            Count = page.Count,
            TotalCount = _ctsCphHoldings.Count,
            Skip = skip,
            Top = take
        };
    }

    public QueryResult ListSamCphHoldings(int skip, int take)
    {
        var count = Math.Min(take, _samCphHoldings.Count - skip);
        var page = count > 0 ? _samCphHoldings.GetRange(skip, count) : [];
        return new QueryResult
        {
            CollectionName = dataSetDefinitions.SamCPHHolding.Name,
            Data = page,
            Count = page.Count,
            TotalCount = _samCphHoldings.Count,
            Skip = skip,
            Top = take
        };
    }

    // ── Private loading methods ─────────────────────────────────────────────

    private async Task LoadCtsGroupAsync(CancellationToken ct,
        OperationScope? holdingsScope = null, int? holdingsCount = null,
        OperationScope? keepersScope = null, int? keepersCount = null,
        Func<bool>? isCancellationRequested = null)
    {
        await LoadCtsCphHoldingsAsync(ct, holdingsScope, holdingsCount, isCancellationRequested);
        await LoadCtsKeepersAsync(ct, keepersScope, keepersCount, isCancellationRequested);
    }

    private async Task LoadSamGroupAsync(CancellationToken ct,
        (OperationScope? Scope, int Count) holdings,
        (OperationScope? Scope, int Count) herds,
        (OperationScope? Scope, int Count) parties,
        Func<bool>? isCancellationRequested = null)
    {
        await LoadSamCphHoldingsAsync(ct, holdings.Scope, holdings.Count, isCancellationRequested);
        await LoadSamHerdsAsync(ct, herds.Scope, herds.Count, isCancellationRequested);
        await LoadSamPartiesAsync(ct, parties.Scope, parties.Count, isCancellationRequested);
    }

    private async Task LoadCtsCphHoldingsAsync(CancellationToken ct, OperationScope? scope = null, int? totalRecords = null, Func<bool>? isCancellationRequested = null)
    {
        scope?.Start(totalRecords, $"Loading {dataSetDefinitions.CTSCPHHolding.Name}");
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsCphHoldings | BEGIN");
        var sw = Stopwatch.StartNew();

        await scope.RunAsync(async () =>
        {
            await foreach (var record in PageAllAsync(dataSetDefinitions.CTSCPHHolding.Name, ct, scope, isCancellationRequested))
            {
                _ctsCphHoldings.Add(record);

                var lid = LidFullIdentifier.TryParse(record.GetValueOrDefault(CtsCphHoldingFields.LidFullIdentifier)?.ToString());
                if (lid is not null)
                {
                    _ctsCphHoldingsByLid.TryAdd(lid.Value, record);
                    _ctsCphHoldingsByCph.TryAdd(lid.Cph.Value, record);
                }
            }
        });

        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsCphHoldings | END, count={_ctsCphHoldings.Count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadCtsKeepersAsync(CancellationToken ct, OperationScope? scope = null, int? totalRecords = null, Func<bool>? isCancellationRequested = null)
    {
        scope?.Start(totalRecords, $"Loading {dataSetDefinitions.CTSKeeper.Name}");
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsKeepers | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await scope.RunAsync(async () =>
        {
            await foreach (var record in PageAllAsync(dataSetDefinitions.CTSKeeper.Name, ct, scope, isCancellationRequested))
            {
                var lid = record.GetValueOrDefault(CtsKeeperFields.LidFullIdentifier)?.ToString();
                if (!string.IsNullOrEmpty(lid))
                {
                    if (!_ctsKeepersByLid.TryGetValue(lid, out var list))
                    {
                        list = [];
                        _ctsKeepersByLid[lid] = list;
                    }
                    list.Add(record);
                    count++;
                }
            }
        });

        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsKeepers | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamCphHoldingsAsync(CancellationToken ct, OperationScope? scope = null, int? totalRecords = null, Func<bool>? isCancellationRequested = null)
    {
        scope?.Start(totalRecords, $"Loading {dataSetDefinitions.SamCPHHolding.Name}");
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHoldings | BEGIN");
        var sw = Stopwatch.StartNew();

        await scope.RunAsync(async () =>
        {
            await foreach (var record in PageAllAsync(dataSetDefinitions.SamCPHHolding.Name, ct, scope, isCancellationRequested))
            {
                _samCphHoldings.Add(record);

                var cph = record.GetValueOrDefault(SamCphHoldingFields.Cph)?.ToString();
                if (!string.IsNullOrEmpty(cph))
                {
                    _samCphHoldingsByCph.TryAdd(cph, record);
                }
            }
        });

        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHoldings | END, count={_samCphHoldings.Count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamHerdsAsync(CancellationToken ct, OperationScope? scope = null, int? totalRecords = null, Func<bool>? isCancellationRequested = null)
    {
        scope?.Start(totalRecords, $"Loading {dataSetDefinitions.SamHerd.Name}");
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamHerds | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await scope.RunAsync(async () =>
        {
            await foreach (var record in PageAllAsync(dataSetDefinitions.SamHerd.Name, ct, scope, isCancellationRequested))
            {
                var cph = ParseCphFromCphh(record.GetValueOrDefault(SamHerd.Cphh)?.ToString());
                if (cph is null)
                {
                    continue;
                }

                if (!_samHerdsByCph.TryGetValue(cph.Value, out var list))
                {
                    list = [];
                    _samHerdsByCph[cph.Value] = list;
                }
                list.Add(record);
                count++;
            }
        });

        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamHerds | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamPartiesAsync(CancellationToken ct, OperationScope? scope = null, int? totalRecords = null, Func<bool>? isCancellationRequested = null)
    {
        scope?.Start(totalRecords, $"Loading {dataSetDefinitions.SamParty.Name}");
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamParties | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await scope.RunAsync(async () =>
        {
            await foreach (var record in PageAllAsync(dataSetDefinitions.SamParty.Name, ct, scope, isCancellationRequested))
            {
                var partyId = record.GetValueOrDefault(SamPartyFields.PartyId)?.ToString();
                if (!string.IsNullOrEmpty(partyId))
                {
                    if (!_samPartiesByPartyId.TryGetValue(partyId, out var list))
                    {
                        list = [];
                        _samPartiesByPartyId[partyId] = list;
                    }
                    list.Add(record);
                    count++;
                }
            }
        });

        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamParties | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamCphHoldersAsync(CancellationToken ct, OperationScope? scope = null, int? totalRecords = null, Func<bool>? isCancellationRequested = null)
    {
        scope?.Start(totalRecords, $"Loading {dataSetDefinitions.SamCPHHolder.Name}");
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHolders | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        // Collect all known CPH values for reverse-indexing
        var allCphValues = new HashSet<string>(
            _ctsCphHoldingsByCph.Keys.Concat(_samCphHoldingsByCph.Keys),
            StringComparer.OrdinalIgnoreCase);

        await scope.RunAsync(async () =>
        {
            await foreach (var record in PageAllAsync(dataSetDefinitions.SamCPHHolder.Name, ct, scope, isCancellationRequested))
            {
                var cphs = record.GetValueOrDefault(SamCphHolderFields.Cphs)?.ToString();
                if (!string.IsNullOrEmpty(cphs))
                {
                    // CPHS is comma-delimited (e.g. "09/236/0027,09/236/0028") — split and match against known CPHs
                    foreach (var segment in cphs.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Where(allCphValues.Contains))
                    {
                        if (!_samCphHoldersByCph.TryGetValue(segment, out var list))
                        {
                            list = [];
                            _samCphHoldersByCph[segment] = list;
                        }
                        list.Add(record);
                    }
                    count++;
                }
            }
        });

        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHolders | END, records={count}, mappings={_samCphHoldersByCph.Values.Sum(v => v.Count)}, duration={sw.ElapsedMilliseconds}ms");
    }

    // ── Shared paging infrastructure ────────────────────────────────────────

    private async IAsyncEnumerable<Dictionary<string, object?>> PageAllAsync(
        string collectionName,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct,
        OperationScope? scope = null,
        Func<bool>? isCancellationRequested = null)
    {
        var skip = 0;

        while (!ct.IsCancellationRequested)
        {
            var settings = throttler.Settings.CleanseAnalysis;

            var (batch, fetchMs) = await Timed.RunAsync(async () =>
            {
                var query = new QueryParameters
                {
                    CollectionName = collectionName,
                    Filter = FilterExpression.Equal(IsDeleted, false),
                    Skip = skip,
                    Top = settings.PumpBatchSize
                };
                return await queryService.QueryAsync(query, ct);
            });
            scope?.TrackElapsed("fetching", fetchMs);

            if (batch.Data.Count == 0)
                break;

            foreach (var record in batch.Data)
                yield return record;

            skip += batch.Data.Count;
            scope?.UpdateProgress(skip);

            // Check for user-initiated cancellation (polled from DB flag)
            if (isCancellationRequested?.Invoke() == true)
                throw new OperationCanceledException("Cancellation requested by user.");

            var delayMs = await Timed.RunAsync(() => throttler.DelayAsync(settings.PumpDelayMs, ct));
            scope?.TrackElapsed("throttle_wait", delayMs);
        }
    }

    private async Task<long> CountCollectionAsync(string collectionName, CancellationToken ct)
    {
        var query = new QueryParameters
        {
            CollectionName = collectionName,
            Filter = FilterExpression.Equal(IsDeleted, false),
            Top = 0,
            IncludeCount = true
        };
        var result = await queryService.QueryAsync(query, ct);
        return result.TotalCount ?? 0;
    }

    private static QueryResult BuildQueryResult(string collectionName, List<Dictionary<string, object?>> data) => new()
    {
        CollectionName = collectionName,
        Data = data,
        Count = data.Count
    };

    /// <summary>
    /// Extracts a CPH from a CPHH string (CC/PPP/HHHH/SS) by stripping the last segment.
    /// </summary>
    private static Cph? ParseCphFromCphh(string? cphh)
    {
        if (string.IsNullOrEmpty(cphh))
        {
            return null;
        }

        var lastSlash = cphh.LastIndexOf('/');
        var cphValue = lastSlash > 0 ? cphh[..lastSlash] : cphh;
        return Cph.TryParse(cphValue);
    }

}
