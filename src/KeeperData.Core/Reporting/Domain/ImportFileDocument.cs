using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Attributes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reporting.Domain;

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
[CollectionName("import_files")]
public class ImportFileDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; init; }

    [BsonRepresentation(BsonType.String)]
    public required Guid ImportId { get; init; }

    public required string FileName { get; init; }
    public required string FileKey { get; init; }
    public required string DatasetName { get; init; }
    public required string ETag { get; init; }
    public long FileSize { get; init; }
    public required string Status { get; init; }

    public FileAcquisitionDetailsDocument? AcquisitionDetails { get; init; }
    public FileIngestionDetailsDocument? IngestionDetails { get; init; }

    public string? Error { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class FileAcquisitionDetailsDocument
{
    public DateTime AcquiredAtUtc { get; init; }
    public required string SourceKey { get; init; }
    public long DecryptionDurationMs { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class FileIngestionDetailsDocument
{
    public DateTime IngestedAtUtc { get; init; }
    public int RecordsProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
    public long IngestionDurationMs { get; init; }
}