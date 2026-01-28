using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Attributes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reporting.Domain;

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
[CollectionName("import_reports")]
public class ImportReportDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public required Guid ImportId { get; set; }

    public required string SourceType { get; set; }
    public required string Status { get; set; }
    public DateTime StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }

    public AcquisitionPhaseDocument? AcquisitionPhase { get; set; }
    public IngestionPhaseDocument? IngestionPhase { get; set; }

    public string? Error { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class AcquisitionPhaseDocument
{
    public required string Status { get; set; }
    public int FilesDiscovered { get; set; }
    public int FilesProcessed { get; set; }
    public int FilesFailed { get; set; }
    public int FilesSkipped { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class IngestionPhaseDocument
{
    public required string Status { get; set; }
    public int FilesProcessed { get; set; }
    public int FilesSkipped { get; set; }
    public int RecordsCreated { get; set; }
    public int RecordsUpdated { get; set; }
    public int RecordsDeleted { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
    public IngestionCurrentFileStatusDocument? CurrentFileStatus { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class IngestionCurrentFileStatusDocument
{
    public string? FileName { get; set; }
    public int? TotalRows { get; set; }
    public int? RowNumber { get; set; }
    public int? PercentageCompleted { get; set; }
    public decimal? RowsPerMinute { get; set; }
    public TimeSpan? EstimatedTimeRemaining { get; set; }
    public DateTime? EstimatedCompletionUtc { get; set; }
}