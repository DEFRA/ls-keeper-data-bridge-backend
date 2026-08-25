using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Models;

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class StartEtlImportResponse
{
    public Guid ImportId { get; set; }
    public required string Status { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlImportConflictResponse
{
    public required string Message { get; set; }

    /// <summary>The import already running, so the caller can poll that one instead.</summary>
    public Guid? InFlightImportId { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlImportStatusResponse
{
    public Guid ImportId { get; set; }
    public required string Status { get; set; }
    public required string SourceType { get; set; }

    /// <summary>The dataset the run was restricted to, or null for all of them.</summary>
    public string? Dataset { get; set; }

    public DateTime RequestedAtUtc { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
    public string? CurrentStage { get; set; }

    public List<EtlImportStageResponse> Stages { get; set; } = [];
    public List<EtlImportDatasetResponse> Datasets { get; set; } = [];

    /// <summary>Key of the staging database this run produced. Ask the staging endpoint for a
    /// presigned URL to download it.</summary>
    public string? DuckDbPath { get; set; }

    /// <summary>Key of the SQLite read model this run produced. Ask the staging endpoint for a
    /// presigned URL to download it.</summary>
    public string? SqlitePath { get; set; }

    /// <summary>Row counts per table in the SQLite read model.</summary>
    public List<EtlImportViewTableResponse> SqliteTables { get; set; } = [];

    /// <summary>A summary safe to show a caller: never a stack trace or a configuration value.</summary>
    public string? Error { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlImportViewTableResponse
{
    public required string Name { get; set; }
    public long RowCount { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlImportStageResponse
{
    public required string Name { get; set; }
    public int ItemCount { get; set; }
    public long ElapsedMs { get; set; }
    public DateTime CompletedAtUtc { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlImportDatasetResponse
{
    public required string Dataset { get; set; }
    public List<EtlImportSourceFileResponse> SourceFiles { get; set; } = [];
    public List<string> RawPaths { get; set; } = [];
    public List<string> NormalisedPaths { get; set; } = [];
    public string? SnapshotPath { get; set; }
    public DateTime? SnapshotSourceTimestampUtc { get; set; }
    public long? RowCount { get; set; }
    public long? RowsUpserted { get; set; }
    public long? RowsIgnoredDeletes { get; set; }
    public List<string> ColumnsNullified { get; set; } = [];
/// <summary>Columns a file introduced, so they are null for the rows held before it.</summary>
    public List<string> ColumnsAdded { get; set; } = [];
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlImportSourceFileResponse
{
    public required string Key { get; set; }
    public long Size { get; set; }
}

/// <summary>A page of imports, most recent first.</summary>
[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlImportListResponse
{
    public int Skip { get; set; }
    public int Top { get; set; }

    /// <summary>How many imports this page contains.</summary>
    public int Count { get; set; }

    /// <summary>How many imports exist in total, for paging.</summary>
    public long TotalCount { get; set; }

    public List<EtlImportSummaryResponse> Imports { get; set; } = [];
}

/// <summary>Enough of an import to list it and link to it. The per-dataset detail - source files,
/// raw, normalised and snapshot paths - is on the by-id endpoint, so a page of imports stays small
/// however many datasets each run touched.</summary>
[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlImportSummaryResponse
{
    public Guid ImportId { get; set; }
    public required string Status { get; set; }
    public required string SourceType { get; set; }

    /// <summary>The dataset the run was restricted to, or null for all of them.</summary>
    public string? Dataset { get; set; }

    public DateTime RequestedAtUtc { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
    public string? CurrentStage { get; set; }

    /// <summary>Datasets the run produced output for.</summary>
    public int DatasetCount { get; set; }

    /// <summary>Source files discovered across every dataset. Zero on a succeeded run means nothing
    /// was found to process - usually a filename timestamp outside the discovery window.</summary>
    public int SourceFileCount { get; set; }

    /// <summary>Rows summed across datasets, where the run got far enough to count any.</summary>
    public long? RowCount { get; set; }

    public string? DuckDbPath { get; set; }

    public string? SqlitePath { get; set; }

    public string? Error { get; set; }
}
