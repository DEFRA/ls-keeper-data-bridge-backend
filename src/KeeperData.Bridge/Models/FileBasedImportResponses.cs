using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Models;

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class StartFileBasedImportResponse
{
    public Guid ImportId { get; set; }
    public required string Status { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class FileBasedImportConflictResponse
{
    public required string Message { get; set; }

    /// <summary>The import already running, so the caller can poll that one instead.</summary>
    public Guid? InFlightImportId { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class FileBasedImportStatusResponse
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

    public List<FileBasedImportStageResponse> Stages { get; set; } = [];
    public List<FileBasedImportDatasetResponse> Datasets { get; set; } = [];

    /// <summary>Key of the staging database this run produced. Ask the staging endpoint for a
    /// presigned URL to download it.</summary>
    public string? DuckDbPath { get; set; }

    /// <summary>A summary safe to show a caller: never a stack trace or a configuration value.</summary>
    public string? Error { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class FileBasedImportStageResponse
{
    public required string Name { get; set; }
    public int ItemCount { get; set; }
    public long ElapsedMs { get; set; }
    public DateTime CompletedAtUtc { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class FileBasedImportDatasetResponse
{
    public required string Dataset { get; set; }
    public List<FileBasedImportSourceFileResponse> SourceFiles { get; set; } = [];
    public List<string> RawPaths { get; set; } = [];
    public List<string> NormalisedPaths { get; set; } = [];
    public string? SnapshotPath { get; set; }
    public DateTime? SnapshotSourceTimestampUtc { get; set; }
    public long? RowCount { get; set; }
    public long? RowsUpserted { get; set; }
    public long? RowsIgnoredDeletes { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class FileBasedImportSourceFileResponse
{
    public required string Key { get; set; }
    public long Size { get; set; }
}
