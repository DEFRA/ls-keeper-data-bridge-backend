using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// Translates between a dataset definition's naming convention and the storage keys of its files,
/// so that every catalogue implementation agrees on what a dataset's files are called.
/// </summary>
public static class DataSetFileNaming
{
    private static readonly ConcurrentDictionary<string, Regex[]> s_globs = new(StringComparer.Ordinal);
    // Limit regex execution time to avoid pathological patterns causing long-running matches.
    private static readonly TimeSpan s_regexTimeout = TimeSpan.FromMilliseconds(200);
    private static readonly Regex s_run = new(@"_(\d{5})_\d{3}_", RegexOptions.CultureInvariant, s_regexTimeout);

    /// <summary>
    /// The prefixes storage must be listed under to see every file in the dataset. A glob dataset
    /// yields the literal head of each of its lanes, so the lanes stay independently addressable
    /// and neither listing carries the other; anything else yields its single literal prefix.
    /// </summary>
    public static IReadOnlyList<string> ListingPrefixes(DataSetDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        if (definition.SourceKeyPattern is null)
        {
            return [DataSetKeyPrefix(definition)];
        }

        return [.. Lanes(definition.SourceKeyPattern).Select(LiteralHead).Distinct(StringComparer.Ordinal)];
    }

    /// <summary>
    /// Whether a storage key belongs to the dataset. A glob dataset matches its pattern; anything
    /// else matches its literal prefix, which is what listing under it would already have returned.
    /// </summary>
    public static bool Matches(DataSetDefinition definition, string key)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (definition.SourceKeyPattern is null)
        {
            return key.StartsWith(DataSetKeyPrefix(definition), StringComparison.OrdinalIgnoreCase);
        }

        return MatchesPattern(definition.SourceKeyPattern, key);
    }

    /// <summary>
    /// Whether a key belongs to the dataset's baseline (bulk) lane. A dataset with no baseline pattern
    /// has an empty bulk set, which is every dataset but CTS.
    ///
    /// The key is tried whole and then by its final segment, so a source key that carries the lane
    /// folder and a normalised key that has lost it both answer the same.
    /// </summary>
    public static bool MatchesBaseline(DataSetDefinition definition, string key)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (definition.BaselineKeyPattern is null)
        {
            return false;
        }

        return MatchesPattern(definition.BaselineKeyPattern, key)
            || MatchesPattern(FinalSegment(definition.BaselineKeyPattern), FinalSegment(key));
    }

    /// <summary>
    /// Whether a key sits in the lane the dataset publishes its baselines to, judged by the folder
    /// alone, so every key in that lane answers the same regardless of how it is named.
    /// </summary>
    public static bool InBaselineLane(DataSetDefinition definition, string key)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (definition.BaselineKeyPattern is null)
        {
            return false;
        }

        return Lanes(definition.BaselineKeyPattern)
            .Select(LiteralHead)
            .Any(head => key.StartsWith(head, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// The extract run a file was cut by, where its name carries one. CTS numbers every run of its
    /// extract and splits a large table across parts within that run, naming the pair
    /// <c>_&lt;run&gt;_&lt;part&gt;_</c> in fixed-width digits, so runs compare as text. Null for a name
    /// that carries no such pair, which makes a dataset naming its files any other way one
    /// undifferentiated set.
    /// </summary>
    public static string? ExtractRun(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        var match = s_run.Match(FinalSegment(key));

        return match.Success ? match.Groups[1].Value : null;
    }

    private static string FinalSegment(string value) => value[(value.LastIndexOf('/') + 1)..];

    private static bool MatchesPattern(string pattern, string key)
        => s_globs.GetOrAdd(pattern, CompileGlob).Any(lane => lane.IsMatch(key));

    private static Regex[] CompileGlob(string pattern)
    {
        return [.. Lanes(pattern).Select(lane => new Regex(
            GlobRegex(lane),
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
            s_regexTimeout))];
    }

    /// <summary>
    /// A pattern's alternative folders, so that one dataset spanning two lanes can be written as
    /// <c>cads/cts/{bulk,daily}/*NAME*.csv</c> rather than as two patterns.
    /// </summary>
    private static IReadOnlyList<string> Lanes(string pattern)
    {
        var open = pattern.IndexOf('{');
        var close = open < 0 ? -1 : pattern.IndexOf('}', open);

        if (open < 0 || close < 0)
        {
            return [pattern];
        }

        var head = pattern[..open];
        var tail = pattern[(close + 1)..];

        return [.. pattern[(open + 1)..close]
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(alternative => $"{head}{alternative}{tail}")];
    }

    private static string LiteralHead(string pattern)
    {
        var firstWildcard = pattern.IndexOf('*');

        return firstWildcard < 0 ? pattern : pattern[..firstWildcard];
    }

    /// <summary>
    /// Deliberately small: literals, <c>*</c> within a path segment, and <c>**</c> across segments.
    /// No character classes and no single-character wildcard.
    /// </summary>
    private static string GlobRegex(string pattern)
    {
        var regex = new StringBuilder("^");

        // Replace double-star sequences with a single sentinel character so the loop
        // can handle single- and double-star cases without mutating the loop index.
        const char DoubleStarSentinel = '\u0000';
        var normalized = pattern.Replace("**", DoubleStarSentinel.ToString());

        for (var index = 0; index < normalized.Length; index++)
        {
            var ch = normalized[index];

            if (ch == DoubleStarSentinel)
            {
                regex.Append(".*");
                continue;
            }

            if (ch == '*')
            {
                regex.Append("[^/]*");
                continue;
            }

            regex.Append(Regex.Escape(ch.ToString()));
        }

        return regex.Append('$').ToString();
    }

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

        TimeOnly DefaultSetAsNoon = new(12, 0, 0);

        return patternIncludesTime
            ? date.ToDateTime(DefaultSetAsNoon).ToString(definition.DatePattern)
            : date.ToString(definition.DatePattern);
    }

    /// <summary>
    /// Reads the timestamp encoded in the trailing segment of a file's storage key.
    /// </summary>
    /// <exception cref="InvalidOperationException">The key does not carry a parsable timestamp.</exception>
    public static DateTimeOffset ExtractTimestamp(DataSetDefinition definition, string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(definition);

        var timestampLength = definition.DateTimePattern.Length;

        var beforeFirstDot = key.Split('.')[0];
        var underscoreParts = beforeFirstDot.Split('_');
        var timestampPart = underscoreParts[^1];

        if (timestampPart.Length < timestampLength)
        {
            throw new InvalidOperationException($"Cannot extract timestamp from blob key '{key}' for dataset '{definition.Name}'");
        }

        var timestampText = timestampPart[..timestampLength];

        if (!DateTime.TryParseExact(timestampText, definition.DateTimePattern,
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedDateTime))
        {
            throw new InvalidOperationException($"Cannot parse timestamp '{timestampText}' from blob key '{key}' for dataset '{definition.Name}'");
        }

        return new DateTimeOffset(DateTime.SpecifyKind(parsedDateTime, DateTimeKind.Utc), TimeSpan.Zero);
    }
}
