using System.Globalization;

namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// Translates between a dataset definition's naming convention and the storage keys of its files,
/// so that every catalogue implementation agrees on what a dataset's files are called.
/// </summary>
public static class DataSetFileNaming
{
    private const int TimestampLength = 14;
    private static readonly TimeOnly DefaultTimeOfDay = new(12, 0, 0);

    /// <summary>
    /// The key prefix shared by every file in the dataset, regardless of date.
    /// Listing storage under this prefix returns the dataset's entire history.
    /// </summary>
    public static string DataSetKeyPrefix(DataSetDefinition definition)
        => definition.FilePrefixFormat.Replace("{0}", string.Empty);

    /// <summary>
    /// The key prefix shared by the dataset's files for a single date.
    /// Listing storage under this prefix returns one day of that dataset.
    /// </summary>
    public static string DatedKeyPrefix(DataSetDefinition definition, DateOnly date)
        => string.Format(definition.FilePrefixFormat, FormatDate(definition, date));

    private static string FormatDate(DataSetDefinition definition, DateOnly date)
    {
        var patternIncludesTime = definition.DatePattern.Contains('H')
            || definition.DatePattern.Contains('m')
            || definition.DatePattern.Contains('s');

        return patternIncludesTime
            ? date.ToDateTime(DefaultTimeOfDay).ToString(definition.DatePattern)
            : date.ToString(definition.DatePattern);
    }

    /// <summary>
    /// Reads the timestamp encoded in the trailing segment of a file's storage key.
    /// </summary>
    /// <exception cref="InvalidOperationException">The key does not carry a parsable timestamp.</exception>
    public static DateTimeOffset ExtractTimestamp(DataSetDefinition definition, string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key, nameof(key));
        ArgumentNullException.ThrowIfNull(definition, nameof(definition));

        var timestampPart = key.Split(".").First().Split('_').Last();

        if (timestampPart.Length < definition.DateTimePattern.Length || !long.TryParse(timestampPart.AsSpan(0, TimestampLength), out _))
        {
            throw new InvalidOperationException($"Cannot extract timestamp from blob key '{key}' for dataset '{definition.Name}'");
        }

        var timestampText = timestampPart[..TimestampLength];

        if (!DateTime.TryParseExact(timestampText, definition.DateTimePattern,
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedDateTime))
        {
            throw new InvalidOperationException($"Cannot parse timestamp '{timestampText}' from blob key '{key}' for dataset '{definition.Name}'");
        }

        return new DateTimeOffset(DateTime.SpecifyKind(parsedDateTime, DateTimeKind.Utc), TimeSpan.Zero);
    }
}
