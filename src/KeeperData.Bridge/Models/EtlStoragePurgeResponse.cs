using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Models;

/// <summary>Summary of an ETL stage-storage purge.</summary>
[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public sealed class EtlStoragePurgeResponse
{
    public bool Success { get; init; }

    /// <summary>Id of the purge's entry in import history.</summary>
    public Guid PurgeId { get; init; }

    /// <summary>The stages actually purged: the requested stage plus everything downstream of it.</summary>
    public IReadOnlyList<string> PurgedStages { get; init; } = [];

    public int DeletedCount { get; init; }

    public required IReadOnlyList<string> DeletedKeys { get; init; }

    public required string Message { get; init; }

    public DateTime PurgedAtUtc { get; init; }
}
