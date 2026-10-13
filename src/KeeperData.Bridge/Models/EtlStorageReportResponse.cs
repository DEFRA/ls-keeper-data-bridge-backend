using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Models;

/// <summary>A file/size report over the objects in one or more ETL stage folders.</summary>
[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public sealed class EtlStorageReportResponse
{
    /// <summary>The stage requested, or 'all' when the report spans every folder.</summary>
    public required string Stage { get; init; }

    /// <summary>The dataset requested, or 'all'.</summary>
    public required string Dataset { get; init; }

    public required string SourceType { get; init; }

    /// <summary>Objects in the whole listing, not just the returned page.</summary>
    public int ObjectCount { get; init; }

    /// <summary>Bytes in the whole listing, not just the returned page.</summary>
    public long TotalSizeBytes { get; init; }

    /// <summary>Totals per dataset ('shared' for the all-dataset staging/views artefacts).</summary>
    public IReadOnlyList<EtlStorageReportGroup> Groups { get; init; } = [];

    /// <summary>The requested slice of the listing, ordered by key.</summary>
    public required IReadOnlyList<EtlStorageReportObject> Objects { get; init; }

    public int Skip { get; init; }

    public int Top { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public sealed class EtlStorageReportGroup
{
    /// <summary>The dataset owning the objects, 'shared' for the all-dataset staging/views
    /// artefacts, or 'other' for keys matched to neither.</summary>
    public required string Dataset { get; init; }

    public int ObjectCount { get; init; }

    public long SizeBytes { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public sealed class EtlStorageReportObject
{
    /// <summary>The stage folder holding the object, e.g. raw or snapshots.</summary>
    public required string Stage { get; init; }

    /// <summary>The object's key relative to the stage folder.</summary>
    public required string Key { get; init; }

    public long SizeBytes { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }
}
