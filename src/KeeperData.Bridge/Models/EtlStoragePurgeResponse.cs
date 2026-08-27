using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Models;

/// <summary>Summary of an ETL stage-storage purge.</summary>
[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public sealed class EtlStoragePurgeResponse
{
    public bool Success { get; init; }

    public int DeletedCount { get; init; }

    public required IReadOnlyList<string> DeletedKeys { get; init; }

    public required string Message { get; init; }

    public DateTime PurgedAtUtc { get; init; }
}
