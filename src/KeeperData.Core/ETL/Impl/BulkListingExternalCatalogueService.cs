using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// Improved bulk listing catalogue service:
/// Discovers source files by listing each dataset's whole prefix once and selecting the requested
/// dates in memory, rather than listing storage again for every date in the range.
///
/// The listing streams, and a key is filtered against the dataset's pattern before its timestamp
/// is read: a glob dataset shares its folder with other tables whose names carry no timestamp this
/// dataset could parse, and only the matched keys are ever held.
/// </summary>
public class BulkListingExternalCatalogueService(IBlobStorageServiceReadOnly sourceBlobs,
    TimeProvider timeProvider,
    IDataSetDefinitions dataSetDefinitions,
    ILogger<BulkListingExternalCatalogueService>? logger = null) : IExternalCatalogueService
{
    private const int MaxConcurrentDataSetListings = 10;

    private readonly ILogger _logger = logger ?? NullLogger<BulkListingExternalCatalogueService>.Instance;

    public Task<ImmutableList<FileSet>> GetFileSetsAsync(int days, CancellationToken ct)
    {
        var today = DateOnly.FromDateTime(timeProvider.GetUtcNow().DateTime);
        var from = days == 0 ? today : today.AddDays(-days + 1);

        return GetFileSetsAsync(dataSetDefinitions.All, from, today, ct);
    }

    public Task<ImmutableList<FileSet>> GetFileSetsAsync(DateOnly date, CancellationToken ct)
        => GetFileSetsAsync(dataSetDefinitions.All, date, date, ct);

    public Task<ImmutableList<FileSet>> GetFileSetsAsync(DateOnly from, DateOnly to, CancellationToken ct)
        => GetFileSetsAsync(dataSetDefinitions.All, from, to, ct);

    public Task<ImmutableList<FileSet>> GetFileSetsAsync(ImmutableArray<DataSetDefinition> definitions, DateOnly date, CancellationToken ct)
        => GetFileSetsAsync(definitions, date, date, ct);

    public async Task<ImmutableList<FileSet>> GetFileSetsAsync(ImmutableArray<DataSetDefinition> definitions, DateOnly from, DateOnly to, CancellationToken ct)
    {
        var fileSetsByDefinition = new ConcurrentDictionary<DataSetDefinition, FileSet>();

        ParallelOptions parallelOptions = ParallelOptions(ct);
        await Parallel.ForEachAsync(definitions,
            parallelOptions,
            async (definition, listingToken) =>
            {
                fileSetsByDefinition[definition] = await GetFileSetAsync(definition, from, to, listingToken);
            });

        return [.. definitions.Select(definition => fileSetsByDefinition[definition])];
    }

    /// <summary>Lists the reduced prefix set once and offers every key to every definition, so a
    /// lane shared by six datasets is read once rather than six times.</summary>
    public async Task<ImmutableList<FileSet>> GetAllFileSetsAsync(
        ImmutableArray<DataSetDefinition> definitions, CancellationToken ct)
    {
        var prefixes = DataSetFileNaming.ListingPrefixes(definitions);
        var matches = new ConcurrentBag<List<(DataSetDefinition Definition, EtlFile File)>>();
        var stopwatch = Stopwatch.StartNew();
        var scanned = 0;
        var requests = 0;

        await Parallel.ForEachAsync(prefixes, ParallelOptions(ct), async (prefix, listingToken) =>
        {
            var found = new List<(DataSetDefinition, EtlFile)>();
            var count = 0;

            await foreach (var blob in sourceBlobs.EnumerateAsync(prefix, listingToken))
            {
                count++;

                foreach (var definition in definitions)
                {
                    // Matched before the timestamp is read: a lane holds tables this dataset cannot
                    // parse a timestamp from, and ExtractTimestamp throws rather than skipping.
                    if (!DataSetFileNaming.Matches(definition, blob.Key)) continue;

                    found.Add((definition, new EtlFile(blob, DataSetFileNaming.ExtractTimestamp(definition, blob.Key))));
                }
            }

            Interlocked.Add(ref scanned, count);
            Interlocked.Add(ref requests, Pages(count));
            matches.Add(found);

            _logger.LogDebug(
                "Prefix {Prefix}: {ScannedCount} object(s) in {PageCount} listing request(s)",
                prefix, count, Pages(count));
        });

        var byDefinition = matches
            .SelectMany(found => found)
            .GroupBy(match => match.Definition)
            .ToDictionary(group => group.Key, group => group.Select(match => match.File).ToList());

        var fileSets = definitions
            .Select(definition => new FileSet(definition, byDefinition.TryGetValue(definition, out var files)
                ? [.. files.OrderBy(file => file.Timestamp)]
                : []))
            .ToImmutableList();

        _logger.LogInformation(
            "Listed {PrefixCount} prefix(es) in {ElapsedMs}ms using {RequestCount} storage request(s): " +
            "{ScannedCount} object(s) scanned, {MatchedCount} matched across {DataSetCount} dataset(s)",
            prefixes.Count, stopwatch.ElapsedMilliseconds, requests, scanned,
            fileSets.Sum(set => set.Files.Length), definitions.Length);

        return fileSets;
    }

    /// <summary>Listings page at 1000 keys, so this is the number of requests the prefix cost.</summary>
    private static int Pages(int objects) => Math.Max(1, (objects + 999) / 1000);

    public Task<FileSet> GetFileSetAsync(DataSetDefinition definition, DateOnly date, CancellationToken ct)
        => GetFileSetAsync(definition, date, date, ct);

    public async Task<FileSet> GetFileSetAsync(DataSetDefinition definition, DateOnly from, DateOnly to, CancellationToken ct)
    {
        var files = new List<EtlFile>();

        foreach (var prefix in DataSetFileNaming.ListingPrefixes(definition))
            await CollectMatchingFilesAsync(definition, prefix, from, to, files, ct);

        return new FileSet(definition, [.. files.OrderBy(file => file.Timestamp)]);
    }

    /// <summary>Streams one listing prefix and appends the files it yields that both belong to the
    /// dataset and fall within the requested date range.</summary>
    private async Task CollectMatchingFilesAsync(
        DataSetDefinition definition,
        string prefix,
        DateOnly from,
        DateOnly to,
        List<EtlFile> files,
        CancellationToken ct)
    {
        await foreach (var blob in sourceBlobs.EnumerateAsync(prefix, ct))
        {
            if (!DataSetFileNaming.Matches(definition, blob.Key))
            {
                continue;
            }

            var file = new EtlFile(blob, DataSetFileNaming.ExtractTimestamp(definition, blob.Key));

            if (FallsWithin(from, to, file))
            {
                files.Add(file);
            }
        }
    }

    private static bool FallsWithin(DateOnly from, DateOnly to, EtlFile file)
    {
        var fileDate = DateOnly.FromDateTime(file.Timestamp.UtcDateTime);

        return fileDate >= from && fileDate <= to;
    }

    public override string ToString() => $"{nameof(BulkListingExternalCatalogueService)}[{sourceBlobs}]";

    ParallelOptions ParallelOptions(CancellationToken cancellationToken) =>
        new()
        {
            MaxDegreeOfParallelism = MaxConcurrentDataSetListings,
            CancellationToken = cancellationToken
        };
}
