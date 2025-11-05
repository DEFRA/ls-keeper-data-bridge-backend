namespace KeeperData.Core.Reporting.Services;

/// <summary>
/// Service responsible for generating composite IDs for lineage tracking.
/// Follows Single Responsibility Principle - only handles ID generation logic.
/// </summary>
public interface ILineageIdGenerator
{
    /// <summary>
    /// Generates a composite ID for a lineage document.
    /// Format: {CollectionName}__{RecordId}
    /// </summary>
    string GenerateLineageDocumentId(string collectionName, string recordId);

    /// <summary>
    /// Generates a chronologically-ordered composite ID for a lineage event.
    /// Format: {CollectionName}__{RecordId}__{yyyyMMddHHmmssffffff}__{NNNNNN}
    /// The timestamp and random components ensure uniqueness and natural chronological sorting.
    /// </summary>
    string GenerateLineageEventId(string collectionName, string recordId, DateTime eventDateUtc);
}

/// <summary>
/// Default implementation of lineage ID generation.
/// Pure functions with no side effects.
/// </summary>
public class LineageIdGenerator : ILineageIdGenerator
{
    private const string Delimiter = "__";
    private const string TimestampFormat = "yyyyMMddHHmmssffffff"; // 20 chars, microsecond precision
    private const int RandomDigits = 6; // 000000-999999

    public string GenerateLineageDocumentId(string collectionName, string recordId)
    {
        ValidateInput(collectionName, nameof(collectionName));
        ValidateInput(recordId, nameof(recordId));

        return $"{collectionName}{Delimiter}{recordId}";
    }

    public string GenerateLineageEventId(string collectionName, string recordId, DateTime eventDateUtc)
    {
        ValidateInput(collectionName, nameof(collectionName));
        ValidateInput(recordId, nameof(recordId));

        var lineageDocId = GenerateLineageDocumentId(collectionName, recordId);
        var timestamp = eventDateUtc.ToString(TimestampFormat);
        var random = Random.Shared.Next(0, 1_000_000).ToString($"D{RandomDigits}");

        return $"{lineageDocId}{Delimiter}{timestamp}{Delimiter}{random}";
    }

    private static void ValidateInput(string value, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{paramName} cannot be null or whitespace.", paramName);
        }

        if (value.Contains(Delimiter))
        {
            throw new ArgumentException($"{paramName} cannot contain the delimiter '{Delimiter}'.", paramName);
        }
    }
}