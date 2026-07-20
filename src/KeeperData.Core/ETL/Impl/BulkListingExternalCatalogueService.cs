using System.Collections.Concurrent;
using System.Collections.Immutable;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// Discovers source files by listing each dataset's whole prefix once and selecting the requested
/// dates in memory, rather than listing storage again for every date in the range. The number of
/// storage listings is bounded by the dataset count and does not grow with the lookback window.
///
/// Every result holds one file set per requested definition - including definitions with no
/// matching files - with the files ordered by timestamp ascending.
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

        await Parallel.ForEachAsync(definitions,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = MaxConcurrentDataSetListings,
                CancellationToken = ct
            },
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
        var blobs = await sourceBlobs.ListAsync(DataSetFileNaming.DataSetKeyPrefix(definition), ct);

        return new FileSet(definition, SelectFilesWithin(from, to, definition, blobs));
    }

    private static EtlFile[] SelectFilesWithin(DateOnly from, DateOnly to, DataSetDefinition definition, IReadOnlyList<StorageObjectInfo> blobs)
        => [.. blobs
            .Select(blob => new EtlFile(blob, DataSetFileNaming.ExtractTimestamp(definition, blob.Key)))
            .Where(file => FallsWithin(from, to, file))
            .OrderBy(file => file.Timestamp)];

    private static bool FallsWithin(DateOnly from, DateOnly to, EtlFile file)
    {
        var fileDate = DateOnly.FromDateTime(file.Timestamp.UtcDateTime);

        return fileDate >= from && fileDate <= to;
    }

    public override string ToString() => $"{nameof(BulkListingExternalCatalogueService)}[{sourceBlobs}]";
}
