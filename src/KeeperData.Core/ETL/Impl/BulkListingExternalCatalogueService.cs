using System.Collections.Concurrent;
using System.Collections.Immutable;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;

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
    IDataSetDefinitions dataSetDefinitions) : IExternalCatalogueService
{
    private const int MaxConcurrentDataSetListings = 10;

    public Task<ImmutableList<FileSet>> GetFileSetsAsync(CancellationToken ct)
        => GetFileSetsAsync(days: 0, ct);

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
