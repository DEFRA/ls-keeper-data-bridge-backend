using System.Diagnostics;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
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

    public async Task PreloadAsync(TimingTree timings, CancellationToken ct)
    {
        if (_loaded)
            throw new InvalidOperationException("PreloadAsync has already been called. This service instance cannot be preloaded more than once.");
        _loaded = true;

        Trace.WriteLine("KRDSBRIDGE | PreloadedCtsSamDataService | PreloadAsync | BEGIN");
        var sw = Stopwatch.StartNew();

        // CTS and SAM collections are independent — load them in parallel
        var ctsTimings = new TimingTree();
        var samTimings = new TimingTree();
        await Task.WhenAll(
            LoadCtsGroupAsync(ctsTimings, ct),
            LoadSamGroupAsync(samTimings, ct));
        timings.Merge(ctsTimings, "");
        timings.Merge(samTimings, "");

        // Holders depend on both CTS + SAM CPH keys being populated
        await LoadSamCphHoldersAsync(timings, ct);

        sw.Stop();
        Trace.WriteLine($"KRDSBRIDGE | PreloadedCtsSamDataService | PreloadAsync | END, duration={sw.ElapsedMilliseconds}ms");
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

    private async Task LoadCtsGroupAsync(TimingTree timings, CancellationToken ct)
    {
        await LoadCtsCphHoldingsAsync(timings, ct);
        await LoadCtsKeepersAsync(timings, ct);
    }

    private async Task LoadSamGroupAsync(TimingTree timings, CancellationToken ct)
    {
        await LoadSamCphHoldingsAsync(timings, ct);
        await LoadSamHerdsAsync(timings, ct);
        await LoadSamPartiesAsync(timings, ct);
    }

    private async Task LoadCtsCphHoldingsAsync(TimingTree timings, CancellationToken ct)
    {
        Trace.WriteLine("KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsCphHoldings | BEGIN");
        var sw = Stopwatch.StartNew();

        await foreach (var record in PageAllAsync(dataSetDefinitions.CTSCPHHolding.Name, timings, "Preload/CtsCphHoldings", ct))
        {
            _ctsCphHoldings.Add(record);

            var lid = LidFullIdentifier.TryParse(record.GetValueOrDefault(CtsCphHoldingFields.LidFullIdentifier)?.ToString());
            if (lid is not null)
            {
                _ctsCphHoldingsByLid.TryAdd(lid.Value, record);
                _ctsCphHoldingsByCph.TryAdd(lid.Cph.Value, record);
            }
        }

        sw.Stop();
        timings.Track("Preload/CtsCphHoldings/total", sw.ElapsedMilliseconds);
        Trace.WriteLine($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsCphHoldings | END, count={_ctsCphHoldings.Count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadCtsKeepersAsync(TimingTree timings, CancellationToken ct)
    {
        Trace.WriteLine("KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsKeepers | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await foreach (var record in PageAllAsync(dataSetDefinitions.CTSKeeper.Name, timings, "Preload/CtsKeepers", ct))
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

        sw.Stop();
        timings.Track("Preload/CtsKeepers/total", sw.ElapsedMilliseconds);
        Trace.WriteLine($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadCtsKeepers | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamCphHoldingsAsync(TimingTree timings, CancellationToken ct)
    {
        Trace.WriteLine("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHoldings | BEGIN");
        var sw = Stopwatch.StartNew();

        await foreach (var record in PageAllAsync(dataSetDefinitions.SamCPHHolding.Name, timings, "Preload/SamCphHoldings", ct))
        {
            _samCphHoldings.Add(record);

            var cph = record.GetValueOrDefault(SamCphHoldingFields.Cph)?.ToString();
            if (!string.IsNullOrEmpty(cph))
            {
                _samCphHoldingsByCph.TryAdd(cph, record);
            }
        }

        sw.Stop();
        timings.Track("Preload/SamCphHoldings/total", sw.ElapsedMilliseconds);
        Trace.WriteLine($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHoldings | END, count={_samCphHoldings.Count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamHerdsAsync(TimingTree timings, CancellationToken ct)
    {
        Trace.WriteLine("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamHerds | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await foreach (var record in PageAllAsync(dataSetDefinitions.SamHerd.Name, timings, "Preload/SamHerds", ct))
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

        sw.Stop();
        timings.Track("Preload/SamHerds/total", sw.ElapsedMilliseconds);
        Trace.WriteLine($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamHerds | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamPartiesAsync(TimingTree timings, CancellationToken ct)
    {
        Trace.WriteLine("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamParties | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        await foreach (var record in PageAllAsync(dataSetDefinitions.SamParty.Name, timings, "Preload/SamParties", ct))
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

        sw.Stop();
        timings.Track("Preload/SamParties/total", sw.ElapsedMilliseconds);
        Trace.WriteLine($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamParties | END, count={count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private async Task LoadSamCphHoldersAsync(TimingTree timings, CancellationToken ct)
    {
        Trace.WriteLine("KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHolders | BEGIN");
        var sw = Stopwatch.StartNew();
        var count = 0;

        // Collect all known CPH values for reverse-indexing
        var allCphValues = new HashSet<string>(
            _ctsCphHoldingsByCph.Keys.Concat(_samCphHoldingsByCph.Keys),
            StringComparer.OrdinalIgnoreCase);

        await foreach (var record in PageAllAsync(dataSetDefinitions.SamCPHHolder.Name, timings, "Preload/SamCphHolders", ct))
        {
            var cphs = record.GetValueOrDefault(SamCphHolderFields.Cphs)?.ToString();
            if (!string.IsNullOrEmpty(cphs))
            {
                // CPHS is comma-delimited (e.g. "09/236/0027,09/236/0028") — split and match against known CPHs
                foreach (var segment in cphs.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    if (allCphValues.Contains(segment))
                    {
                        if (!_samCphHoldersByCph.TryGetValue(segment, out var list))
                        {
                            list = [];
                            _samCphHoldersByCph[segment] = list;
                        }
                        list.Add(record);
                    }
                }
                count++;
            }
        }

        sw.Stop();
        timings.Track("Preload/SamCphHolders/total", sw.ElapsedMilliseconds);
        Trace.WriteLine($"KRDSBRIDGE | PreloadedCtsSamDataService | LoadSamCphHolders | END, records={count}, mappings={_samCphHoldersByCph.Values.Sum(v => v.Count)}, duration={sw.ElapsedMilliseconds}ms");
    }

    // ── Shared paging infrastructure ────────────────────────────────────────

    private async IAsyncEnumerable<Dictionary<string, object?>> PageAllAsync(
        string collectionName,
        TimingTree timings,
        string timingPrefix,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
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
            timings.Track($"{timingPrefix}/fetching", batchSw.ElapsedMilliseconds);

            if (batch.Data.Count == 0)
                break;

            foreach (var record in batch.Data)
                yield return record;

            skip += batch.Data.Count;

            batchSw.Restart();
            await throttler.DelayAsync(settings.PumpDelayMs, ct);
            batchSw.Stop();
            timings.Track($"{timingPrefix}/throttle_wait", batchSw.ElapsedMilliseconds);
        }
    }

    private static QueryResult BuildQueryResult(string collectionName, List<Dictionary<string, object?>> data) => new()
    {
        CollectionName = collectionName,
        Data = data,
        Count = data.Count
    };
}
