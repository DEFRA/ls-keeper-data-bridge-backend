using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>A dataset file paired with the source timestamp read from its name.</summary>
public sealed record TimestampedKey(string Key, DateTimeOffset Timestamp);

/// <summary>
/// Naming convention for the files a dataset owns inside the <see cref="EtlPipelineFolders.Normalised"/>
/// and <see cref="EtlPipelineFolders.Snapshots"/> folders. Keys are relative to those folders, because
/// <see cref="IEtlPipelineStorageProvider.ForFolder"/> hands out storage already rooted at one of them.
/// </summary>
public static class SnapshotFileNaming
{
    public const string ParquetExtension = ".parquet";

    public const string ParquetContentType = "application/vnd.apache.parquet";

    /// <summary>The prefix holding every file for a dataset, e.g. <c>sam_cph_holdings/</c>.</summary>
    public static string DataSetPrefix(DataSetDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        return $"{definition.Name}/";
    }

    /// <summary>The key of a dataset's snapshot for the latest source timestamp it includes,
    /// e.g. <c>sam_cph_holdings/sam_cph_holdings_20260728112233.parquet</c>.</summary>
    public static string SnapshotKey(DataSetDefinition definition, DateTimeOffset timestamp)
    {
        ArgumentNullException.ThrowIfNull(definition);

        var stamp = timestamp.UtcDateTime.ToString(definition.DateTimePattern);

        return $"{DataSetPrefix(definition)}{definition.Name}_{stamp}{ParquetExtension}";
    }

    /// <summary>
    /// The newest key by the timestamp encoded in its name, ties broken by ordinal key order.
    /// Keys whose timestamp cannot be parsed are ignored. Returns null when nothing is usable.
    /// </summary>
    public static string? LatestByTimestamp(DataSetDefinition definition, IEnumerable<string> keys)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(keys);

        string? latestKey = null;
        var latestTimestamp = DateTimeOffset.MinValue;

        foreach (var key in keys)
        {
            if (!TryExtractTimestamp(definition, key, out var timestamp))
            {
                continue;
            }

            if (latestKey is null
                || timestamp > latestTimestamp
                || (timestamp == latestTimestamp && string.CompareOrdinal(key, latestKey) > 0))
            {
                latestKey = key;
                latestTimestamp = timestamp;
            }
        }

        return latestKey;
    }

    /// <summary>
    /// The dataset's keys ordered oldest first by the source timestamp in their names. Ordering comes
    /// from the file name alone: object modified time, upload time and the ETL run time say nothing
    /// about where a file sits in the sequence.
    /// </summary>
    /// <exception cref="InvalidOperationException">A key carries no parsable timestamp, or two keys
    /// carry the same one and there is no tie-break to choose between them.</exception>
    public static IReadOnlyList<TimestampedKey> OrderedByTimestamp(DataSetDefinition definition, IEnumerable<string> keys)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(keys);

        var ordered = keys
            .Select(key => new TimestampedKey(key, DataSetFileNaming.ExtractTimestamp(definition, key)))
            .OrderBy(item => item.Timestamp)
            .ToList();

        var duplicate = ordered
            .GroupBy(item => item.Timestamp)
            .FirstOrDefault(group => group.Count() > 1);

        return duplicate is null
            ? ordered
            : throw new InvalidOperationException(
                $"Dataset '{definition.Name}' has {duplicate.Count()} files with source timestamp " +
                $"{duplicate.Key.UtcDateTime.ToString(definition.DateTimePattern)} " +
                $"({string.Join(", ", duplicate.Select(item => item.Key))}); there is no rule for which to apply first");
    }

    /// <summary>Non-throwing form of <see cref="DataSetFileNaming.ExtractTimestamp"/>.</summary>
    public static bool TryExtractTimestamp(DataSetDefinition definition, string key, out DateTimeOffset timestamp)
    {
        ArgumentNullException.ThrowIfNull(definition);

        try
        {
            timestamp = DataSetFileNaming.ExtractTimestamp(definition, key);
            return true;
        }
        catch (Exception exception) when (exception is InvalidOperationException or ArgumentException)
        {
            timestamp = default;
            return false;
        }
    }
}
