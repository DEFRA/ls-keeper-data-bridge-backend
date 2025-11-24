namespace KeeperData.Core.Storage;

/// <summary>
/// Result of a clear down operation containing deleted object keys and count.
/// </summary>
public sealed record ClearDownResult
{
    /// <summary>
    /// List of object keys that were deleted.
    /// </summary>
    public required IReadOnlyList<string> DeletedKeys { get; init; }

    /// <summary>
    /// Total number of objects deleted.
    /// </summary>
    public required int TotalDeleted { get; init; }
}