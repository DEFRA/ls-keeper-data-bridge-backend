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

    /// <summary>Marks the baseline hash segment, so a dataset name ending in hex is not mistaken for one.</summary>
    private const string BaselineHashPrefix = "b";

    /// <summary>The prefix holding every file for a dataset, e.g. <c>sam_cph_holdings/</c>.</summary>
    public static string DataSetPrefix(DataSetDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        return $"{definition.Name}/";
    }

    /// <summary>The key of a dataset's snapshot for the latest source timestamp it includes,
    /// e.g. <c>sam_cph_holdings/sam_cph_holdings_20260728112233.parquet</c>.</summary>
    public static string SnapshotKey(DataSetDefinition definition, DateTimeOffset timestamp)
        => SnapshotKey(definition, timestamp, null);

    /// <summary>
    /// As <see cref="SnapshotKey(DataSetDefinition, DateTimeOffset)"/>, with the baseline hash carried
    /// ahead of the timestamp: <c>cts_location_identifiers/cts_location_identifiers_b1a2b3c4_2026-08-27-063014.parquet</c>.
    ///
    /// The hash belongs in the name rather than in object metadata because a reset can land on the very
    /// timestamp the snapshot it replaces already carries — a new bulk part with no new deltas. Sharing
    /// a key there would meet the stage's refusal to overwrite and silently keep stale data, whereas two
    /// names keep both lineages. Placing it before the timestamp leaves the last <c>_</c> segment alone,
    /// so timestamp extraction is unchanged.
    ///
    /// A null hash — a dataset with no baseline lane — keeps today's unhashed shape, so no existing
    /// snapshot is orphaned.
    /// </summary>
    public static string SnapshotKey(DataSetDefinition definition, DateTimeOffset timestamp, string? baselineHash)
    {
        ArgumentNullException.ThrowIfNull(definition);

        var stamp = timestamp.UtcDateTime.ToString(definition.DateTimePattern);
        var baseline = baselineHash is null ? string.Empty : $"_{BaselineHashPrefix}{baselineHash}";

        return $"{DataSetPrefix(definition)}{definition.Name}{baseline}_{stamp}{ParquetExtension}";
    }

    /// <summary>The baseline hash a snapshot key carries, if any. False for the unhashed shape.</summary>
    public static bool TryExtractBaselineHash(string key, out string? baselineHash)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        baselineHash = null;

        var name = key[(key.LastIndexOf('/') + 1)..].Split('.')[0];
        var segments = name.Split('_');

        if (segments.Length < 2)
        {
            return false;
        }

        var candidate = segments[^2];

        if (candidate.Length != BaselineHash.Length + BaselineHashPrefix.Length
            || !candidate.StartsWith(BaselineHashPrefix, StringComparison.OrdinalIgnoreCase)
            || !candidate[BaselineHashPrefix.Length..].All(Uri.IsHexDigit))
        {
            return false;
        }

        baselineHash = candidate[BaselineHashPrefix.Length..].ToLowerInvariant();

        return true;
    }

    /// <summary>
    /// The newest snapshot built from the given baseline set, or null when none is — which is the whole
    /// reset test: a bulk file added or removed produces a hash no snapshot carries.
    /// </summary>
    public static string? LatestForHash(DataSetDefinition definition, IEnumerable<string> keys, string? baselineHash)
    {
        ArgumentNullException.ThrowIfNull(keys);

        return LatestByTimestamp(definition, keys.Where(key => Carries(key, baselineHash)));
    }

    private static bool Carries(string key, string? baselineHash)
        => TryExtractBaselineHash(key, out var found)
            ? string.Equals(found, baselineHash, StringComparison.OrdinalIgnoreCase)
            : baselineHash is null;

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
