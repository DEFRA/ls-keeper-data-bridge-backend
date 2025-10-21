using KeeperData.Core.Attributes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reporting.Domain;

[CollectionName("import_reports")]
public class ImportReportDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public required Guid ImportId { get; init; }

    public required string SourceType { get; init; }
    public required string Status { get; init; }
    public DateTime StartedAtUtc { get; init; }
    public DateTime? CompletedAtUtc { get; init; }

    public AcquisitionPhaseDocument? AcquisitionPhase { get; init; }
    public IngestionPhaseDocument? IngestionPhase { get; init; }

    public string? Error { get; init; }
}

public class AcquisitionPhaseDocument
{
    public required string Status { get; init; }
    public int FilesDiscovered { get; init; }
    public int FilesProcessed { get; init; }
    public int FilesFailed { get; init; }
    public DateTime? StartedAtUtc { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
}

public class IngestionPhaseDocument
{
    public required string Status { get; init; }
    public int FilesProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
    public DateTime? StartedAtUtc { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
}