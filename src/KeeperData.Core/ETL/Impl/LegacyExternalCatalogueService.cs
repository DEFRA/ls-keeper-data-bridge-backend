using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// The original per-day-scan catalogue: it walks every date in the requested range and issues one
/// storage listing per date per dataset, so the number of round-trips grows with the lookback
/// window (a 250 day lookback across 13 datasets is ~3,250 listings).
/// </summary>
[ExcludeFromCodeCoverage(Justification = "External catalogue service with S3 dependencies - covered by integration tests.")]
public class LegacyExternalCatalogueService(IBlobStorageServiceReadOnly sourceBlobs,
    TimeProvider timeProvider,
    IDataSetDefinitions dataSetDefinitions) : IExternalCatalogueService
{
    public async Task<ImmutableList<FileSet>> GetFileSetsAsync(CancellationToken ct)
    {
        return await GetFileSetsAsync(0, ct);
    }

    public async Task<ImmutableList<FileSet>> GetFileSetsAsync(int days, CancellationToken ct)
    {
        var today = DateOnly.FromDateTime(timeProvider.GetUtcNow().DateTime);

        if (days == 0)
        {
            // For days = 0, return today's files only
            return await GetFileSetsAsync(dataSetDefinitions.All, today, ct);
        }

        // For days > 0, return last n days including today
        var from = today.AddDays(-days + 1);
        return await GetFileSetsAsync(dataSetDefinitions.All, from, today, ct);
    }

    public async Task<ImmutableList<FileSet>> GetFileSetsAsync(DateOnly from, DateOnly to, CancellationToken ct)
    {
        return await GetFileSetsAsync(dataSetDefinitions.All, from, to, ct);
    }

    public async Task<ImmutableList<FileSet>> GetFileSetsAsync(DateOnly date, CancellationToken ct)
    {
        return await GetFileSetsAsync(dataSetDefinitions.All, date, ct);
    }

    public async Task<ImmutableList<FileSet>> GetFileSetsAsync(ImmutableArray<DataSetDefinition> definitions, DateOnly from, DateOnly to, CancellationToken ct)
    {
        var all = new List<ImmutableList<FileSet>>();
        var dates = GetDates(from, to);
        foreach (var date in dates)
        {
            all.Add(await GetFileSetsAsync(definitions, date, ct));
        }

        var list = all.SelectMany(x => x).ToList(); // flattened
        var groupedByDefinition = list.GroupBy(x => x.Definition); // grouped by definition

        // project into new list ordering the files rev-chrono
        var files = groupedByDefinition.Select(x => new FileSet(x.Key,
            [.. x.SelectMany(y => y.Files).OrderBy(x => x.Timestamp)]))
            .ToImmutableList();

        return files;
    }

    public async Task<ImmutableList<FileSet>> GetFileSetsAsync(ImmutableArray<DataSetDefinition> definitions, DateOnly date, CancellationToken ct)
    {
        var list = new ConcurrentBag<FileSet>();

        await Parallel.ForEachAsync(definitions,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = 10,
                CancellationToken = ct
            },
            async (definition, ct) =>
            {
                var fileSet = await GetFileSetAsync(definition, date, ct);
                list.Add(fileSet);
            });

        return [.. list];
    }

    public async Task<FileSet> GetFileSetAsync(DataSetDefinition definition, DateOnly from, DateOnly to, CancellationToken ct)
    {
        var dates = GetDates(from, to);

        var fileSets = new ConcurrentBag<FileSet>();

        await Parallel.ForEachAsync(dates,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = 10,
                CancellationToken = ct
            },
            async (date, ct) =>
            {
                var fileSet = await GetFileSetAsync(definition, date, ct);
                fileSets.Add(fileSet);
            });

        var allFiles = fileSets.SelectMany(fs => fs.Files).ToArray();
        return new FileSet(definition, allFiles);
    }

    public async Task<FileSet> GetFileSetAsync(DataSetDefinition definition, DateOnly date, CancellationToken ct)
    {
        var prefix = DataSetFileNaming.DatedKeyPrefix(definition, date);
        var blobs = await sourceBlobs.ListAsync(prefix, ct);
        var etlFiles = blobs.Select(blob => new EtlFile(blob, DataSetFileNaming.ExtractTimestamp(definition, blob.Key))).ToArray();

        return new FileSet(definition, [.. etlFiles]);
    }

    private static List<DateOnly> GetDates(DateOnly from, DateOnly to)
        => [.. Enumerable.Range(0, to.DayNumber - from.DayNumber + 1).Select(offset => from.AddDays(offset))];

    public override string ToString() => $"{nameof(LegacyExternalCatalogueService)}[{sourceBlobs}]";
}