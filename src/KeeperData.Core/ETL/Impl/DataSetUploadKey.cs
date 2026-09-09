namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// The key an uploaded file is stored under, and whether a dataset claims it.
///
/// A key is judged the way discovery judges one, through the dataset's own pattern, so a file the
/// upload endpoint accepts is a file the pipeline will find. A bare file name is resolved to the
/// folder its dataset lives in, which is how uploads worked before the definitions carried their
/// source folder. Where a dataset spans a baseline lane and a delta lane, the name itself says
/// which it is - a CTS bulk name matches the baseline pattern and nothing else does - so the lane
/// is read off the name rather than guessed, and only a name that leaves the choice genuinely open
/// is refused.
/// </summary>
public static class DataSetUploadKey
{
    public static UploadKeyResolution Resolve(IReadOnlyList<DataSetDefinition> definitions, string objectKey)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        ArgumentException.ThrowIfNullOrWhiteSpace(objectKey);

        if (objectKey.Contains('\\', StringComparison.Ordinal))
        {
            return new UploadKeyResolution(null, "ObjectKey should use '/' to separate folders.");
        }

        if (objectKey.Split('/').Any(segment => segment.Length == 0 || segment is "." or ".."))
        {
            return new UploadKeyResolution(null, $"ObjectKey '{objectKey}' is not a well formed storage key.");
        }

        List<string> candidates = objectKey.Contains('/', StringComparison.Ordinal)
            ? [objectKey]
            : Foldered(definitions, objectKey);

        return candidates.Count switch
        {
            1 => Validate(definitions, candidates[0]),
            0 => new UploadKeyResolution(null, Unmatched(definitions, objectKey)),
            _ => new UploadKeyResolution(null,
                $"'{objectKey}' could belong to more than one folder; supply the full key, one of: {string.Join(", ", candidates)}")
        };
    }

    private static List<string> Foldered(IReadOnlyList<DataSetDefinition> definitions, string fileName)
    {
        return [.. definitions
            .SelectMany(DataSetFileNaming.ListingPrefixes)
            .Select(prefix => prefix[..(prefix.LastIndexOf('/') + 1)])
            .Where(folder => folder.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(folder => folder + fileName)
            .Where(candidate => definitions.Any(definition =>
                DataSetFileNaming.Matches(definition, candidate) && InTheLaneItsNameClaims(definition, candidate)))];
    }

    /// <summary>
    /// Whether a key is in the lane its own name belongs to: a baseline name in the baseline lane,
    /// anything else outside it. A dataset published to one lane has no such choice to make.
    /// </summary>
    private static bool InTheLaneItsNameClaims(DataSetDefinition definition, string key)
        => DataSetFileNaming.MatchesBaseline(definition, key) == DataSetFileNaming.InBaselineLane(definition, key);

    private static UploadKeyResolution Validate(IReadOnlyList<DataSetDefinition> definitions, string key)
    {
        var definition = definitions.FirstOrDefault(definition => DataSetFileNaming.Matches(definition, key));

        if (definition is null)
        {
            return new UploadKeyResolution(null, Unmatched(definitions, key));
        }

        try
        {
            DataSetFileNaming.ExtractTimestamp(definition, key);
        }
        catch (InvalidOperationException exception)
        {
            return new UploadKeyResolution(null, $"'{key}' matches dataset '{definition.Name}' but {exception.Message}");
        }

        return new UploadKeyResolution(key, null);
    }

    private static string Unmatched(IReadOnlyList<DataSetDefinition> definitions, string key)
    {
        var patterns = definitions.Select(definition => definition.SourceKeyPattern
            ?? $"{DataSetFileNaming.DataSetKeyPrefix(definition)}{definition.DateTimePattern}.csv");

        return $"'{key}' does not match any registered dataset. Expected one of: {string.Join(", ", patterns)}";
    }
}

/// <summary>The resolved key, or the reason no key could be settled on.</summary>
public record UploadKeyResolution(string? Key, string? ErrorMessage);
