using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Core.ETL.Impl;

[ExcludeFromCodeCoverage(Justification = "Simple data transfer record.")]
public record EtlFile(StorageObjectInfo StorageObject, DateTimeOffset Timestamp);

[ExcludeFromCodeCoverage(Justification = "Simple data transfer record.")]
public record FileSet(DataSetDefinition Definition, EtlFile[] Files);

[ExcludeFromCodeCoverage(Justification = "External catalogue service with S3 dependencies - covered by integration tests.")]
public class ExternalCatalogueService(IBlobStorageServiceReadOnly sourceBlobs,
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

    private static List<DateOnly> GetDates(DateOnly from, DateOnly to)
        => [.. Enumerable.Range(0, to.DayNumber - from.DayNumber + 1).Select(offset => from.AddDays(offset))];

    public async Task<FileSet> GetFileSetAsync(DataSetDefinition definition, DateOnly date, CancellationToken ct)
    {
        var prefix = GetBlobKeyPrefix(definition, date);
        var blobs = await sourceBlobs.ListAsync(prefix, ct);
        var etlFiles = blobs.Select(blob => new EtlFile(blob, ExtractTimestampFromFileName(definition, blob.Key))).ToArray();

        return new FileSet(definition, [.. etlFiles]);
    }

    private DateTimeOffset ExtractTimestampFromFileName(DataSetDefinition definition, string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key, nameof(key));
        ArgumentNullException.ThrowIfNull(definition, nameof(definition));

        var parts = key.Split(".").First().Split('_');

        if (parts.Length == 0)
        {
            throw new InvalidOperationException($"Cannot extract timestamp from blob key '{key}' for dataset '{definition.Name}'");
        }

        var timestampPart = parts.Last();

        if (timestampPart.Length >= definition.DateTimePattern.Length && long.TryParse(timestampPart.AsSpan(0, 14), out _))
        {
            var dateTimeString = timestampPart.Substring(0, 14);

            if (DateTime.TryParseExact(dateTimeString, definition.DateTimePattern,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None,
                out var parsedDateTime))
            {
                // Create DateTimeOffset from the parsed DateTime, explicitly specifying UTC offset
                var utcDateTime = DateTime.SpecifyKind(parsedDateTime, DateTimeKind.Utc);
                return new DateTimeOffset(utcDateTime, TimeSpan.Zero);
            }
            else
            {
                throw new InvalidOperationException($"Cannot parse timestamp '{dateTimeString}' from blob key '{key}' for dataset '{definition.Name}'");
            }
        }
        else
        {
            throw new InvalidOperationException($"Cannot extract timestamp from blob key '{key}' for dataset '{definition.Name}'");
        }
    }

    private static string GetBlobKeyPrefix(DataSetDefinition definition, DateOnly date)
    {
        string formattedDate;

        // Check if the pattern includes time components (HHmmss or similar)
        if (definition.DatePattern.Contains('H') || definition.DatePattern.Contains('m') || definition.DatePattern.Contains('s'))
        {
            // For patterns that include time, we need to convert to DateTime and add a default time
            var dateTime = date.ToDateTime(new TimeOnly(12, 0, 0)); // Use noon as default time
            formattedDate = dateTime.ToString(definition.DatePattern);
        }
        else
        {
            // For date-only patterns, use DateOnly.ToString()
            formattedDate = date.ToString(definition.DatePattern);
        }

        return string.Format(definition.FilePrefixFormat, formattedDate);
    }

    public override string ToString() => $"{nameof(ExternalCatalogueService)}[{sourceBlobs}]";
}