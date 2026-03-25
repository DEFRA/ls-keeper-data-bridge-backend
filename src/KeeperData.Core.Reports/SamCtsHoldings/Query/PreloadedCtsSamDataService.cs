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

    public async Task PreloadAsync(CancellationToken ct, OperationScope? scope = null)
    {
        if (_loaded)
            throw new InvalidOperationException("PreloadAsync has already been called. This service instance cannot be preloaded more than once.");
        _loaded = true;

        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | PreloadAsync | BEGIN");
        var sw = Stopwatch.StartNew();

        // ── Count all collections upfront so we can report % complete ────────
        var countSw = Stopwatch.StartNew();
        var collectionNames = new[]
        {
            dataSetDefinitions.CTSCPHHolding.Name,
            dataSetDefinitions.CTSKeeper.Name,
            dataSetDefinitions.SamCPHHolding.Name,
            dataSetDefinitions.SamHerd.Name,
            dataSetDefinitions.SamParty.Name,
            dataSetDefinitions.SamCPHHolder.Name
        };
        var countTasks = collectionNames.Select(name => CountCollectionAsync(name, ct)).ToArray();
        await Task.WhenAll(countTasks);
        var counts = countTasks.Select(t => t.Result).ToArray();
        countSw.Stop();
        scope?.TrackElapsed("counting", countSw.ElapsedMilliseconds);

        var totalRecords = counts.Sum();
        scope?.Start((int)totalRecords, $"Loading {totalRecords:N0} records from {collectionNames.Length} collections");
        Trace.TraceInformation($"KRDSBRIDGE | PreloadAsync | Counts retrieved: {string.Join(", ", collectionNames.Zip(counts, (n, c) => $"{n}={c}"))} total={totalRecords}, countDuration={countSw.ElapsedMilliseconds}ms");

        // Create per-collection child scopes
        var collectionScopes = collectionNames.Select(name => scope?.CreateChild(name)).ToArray();
        for (var i = 0; i < collectionScopes.Length; i++)
            collectionScopes[i]?.Start((int)counts[i], $"Loading {collectionNames[i]}");

        // CTS and SAM collections are independent — load them in parallel
        await Task.WhenAll(
            LoadCtsGroupAsync(ct, collectionScopes[0], collectionScopes[1]),
            LoadSamGroupAsync(ct, collectionScopes[2], collectionScopes[3], collectionScopes[4]));

        // Holders depend on both CTS + SAM CPH keys being populated
        await LoadSamCphHoldersAsync(ct, collectionScopes[5]);

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
        OperationScope? holdingsScope = null, OperationScope? keepersScope = null)
    {
        await LoadCtsCphHoldingsAsync(ct, holdingsScope);
        await LoadCtsKeepersAsync(ct, keepersScope);
    }

    private async Task LoadSamGroupAsync(CancellationToken ct,
        OperationScope? holdingsScope = null, OperationScope? herdsScope = null, OperationScope? partiesScope = null)
    {
        await LoadSamCphHoldingsAsync(ct, holdingsScope);
        await LoadSamHerdsAsync(ct, herdsScope);
        await LoadSamPartiesAsync(ct, partiesScope);
    }

    private async Task LoadCtsCphHoldingsAsync(CancellationToken ct, OperationScope? scope = null)
    {
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsCphHoldings | BEGIN");
        var sw = Stopwatch.StartNew();

        await foreach (var record in PageAllAsync(dataSetDefinitions.CTSCPHHolding.Name, ct, scope))
        {
            _ctsCphHoldings.Add(record);

            var lid = LidFullIdentifier.TryParse(record.GetValueOrDefault(CtsCphHoldingFields.LidFullIdentifier)?.ToString());
            if (lid is not null)
            {
                _ctsCphHoldingsByLid.TryAdd(lid.Value, record);
                _ctsCphHoldingsByCph.TryAdd(lid.Cph.Value, record);
            }
        }

        scope?.Complete();
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsCphHoldings | END, count={_ctsCphHoldings.Count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadCtsKeepersAsync(CancellationToken ct, OperationScope? scope = null)
    {
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsKeepers | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await foreach (var record in PageAllAsync(dataSetDefinitions.CTSKeeper.Name, ct, scope))
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

        scope?.Complete();
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsKeepers | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamCphHoldingsAsync(CancellationToken ct, OperationScope? scope = null)
    {
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHoldings | BEGIN");
        var sw = Stopwatch.StartNew();

        await foreach (var record in PageAllAsync(dataSetDefinitions.SamCPHHolding.Name, ct, scope))
        {
            _samCphHoldings.Add(record);

            var cph = record.GetValueOrDefault(SamCphHoldingFields.Cph)?.ToString();
            if (!string.IsNullOrEmpty(cph))
            {
                _samCphHoldingsByCph.TryAdd(cph, record);
            }
        }

        scope?.Complete();
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHoldings | END, count={_samCphHoldings.Count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamHerdsAsync(CancellationToken ct, OperationScope? scope = null)
    {
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamHerds | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await foreach (var record in PageAllAsync(dataSetDefinitions.SamHerd.Name, ct, scope))
        {
            var cphh = record.GetValueOrDefault(SamHerd.Cphh)?.ToString();
            if (!string.IsNullOrEmpty(cphh))
            {
                // CPHH is CC/PPP/HHHH/SS — strip the last segment to get the CPH
                var lastSlash = cphh.LastIndexOf('/');
                var cphValue = lastSlash > 0 ? cphh[..lastSlash] : cphh;
                var cph = Cph.TryParse(cphValue);
                if (cph is not null)
                {
                    if (!_samHerdsByCph.TryGetValue(cph.Value, out var list))
                    {
                        list = [];
                        _samHerdsByCph[cph.Value] = list;
                    }
                    list.Add(record);
                    count++;
                }
            }
        }

        scope?.Complete();
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamHerds | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamPartiesAsync(CancellationToken ct, OperationScope? scope = null)
    {
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamParties | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await foreach (var record in PageAllAsync(dataSetDefinitions.SamParty.Name, ct, scope))
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

        scope?.Complete();
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamParties | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamCphHoldersAsync(CancellationToken ct, OperationScope? scope = null)
    {
        Trace.TraceInformation("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHolders | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        // Collect all known CPH values for reverse-indexing
        var allCphValues = new HashSet<string>(
            _ctsCphHoldingsByCph.Keys.Concat(_samCphHoldingsByCph.Keys),
            StringComparer.OrdinalIgnoreCase);

        await foreach (var record in PageAllAsync(dataSetDefinitions.SamCPHHolder.Name, ct, scope))
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

        scope?.Complete();
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHolders | END, records={count}, mappings={_samCphHoldersByCph.Values.Sum(v => v.Count)}, duration={sw.ElapsedMilliseconds}ms");
    }

    // ── Shared paging infrastructure ────────────────────────────────────────

    private async IAsyncEnumerable<Dictionary<string, object?>> PageAllAsync(
        string collectionName,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct,
        OperationScope? scope = null)
    {
        var skip = 0;
        var batchSw = new Stopwatch();

        while (!ct.IsCancellationRequested)
        {
            var settings = throttler.Settings.CleanseAnalysis;

            batchSw.Restart();
            var query = new QueryParameters
            {
                CollectionName = collectionName,
                Filter = FilterExpression.Equal(IsDeleted, false),
                Skip = skip,
                Top = settings.PumpBatchSize
            };
            var batch = await queryService.QueryAsync(query, ct);
            batchSw.Stop();
            scope?.TrackElapsed("fetching", batchSw.ElapsedMilliseconds);

            if (batch.Data.Count == 0)
                break;

            foreach (var record in batch.Data)
                yield return record;

            skip += batch.Data.Count;
            scope?.UpdateProgress(skip);

            batchSw.Restart();
            await throttler.DelayAsync(settings.PumpDelayMs, ct);
            batchSw.Stop();
            scope?.TrackElapsed("throttle_wait", batchSw.ElapsedMilliseconds);
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

}
