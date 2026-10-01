using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Attributes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.EtlPipeline.Status;

/// <summary>Status of one ETL import.
///
/// A separate collection from the legacy <c>import_reports</c> on purpose: that document is shaped
/// around the legacy acquisition/ingestion phases, and sharing it is how "the existing ETL must not
/// change" gets broken by accident.</summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
[CollectionName("etl_pipeline_imports")]
public class EtlImportDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public required Guid ImportId { get; set; }

    public required string Status { get; set; }

    public required string SourceType { get; set; }

    /// <summary>The dataset the run was restricted to, or null for every configured dataset.</summary>
    public string? Dataset { get; set; }

    public DateTime RequestedAtUtc { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }

    /// <summary>The stage last started. Null before the run begins and after it ends.</summary>
    public string? CurrentStage { get; set; }

    /// <summary>Extended while the run makes progress. A run whose lease has lapsed was abandoned
    /// (the process died mid-run) and is reported as failed rather than left running forever.</summary>
    public DateTime? LeaseExpiresAtUtc { get; set; }

    public List<EtlImportStageDocument> Stages { get; set; } = [];

    public List<EtlImportDatasetDocument> Datasets { get; set; } = [];

    public string? DuckDbKey { get; set; }

    /// <summary>Key of the SQLite read model built from the staging database.</summary>
    public string? SqliteKey { get; set; }

    /// <summary>Row counts per table in the SQLite read model.</summary>
    public List<EtlImportViewTableDocument> SqliteTables { get; set; } = [];

    /// <summary>Exception message only - never a stack trace, and never anything carrying a salt,
    /// password or presigned URL.</summary>
    public string? Error { get; set; }

    /// <summary>Structured context about the failure - which stage, dataset, file or record it
    /// belongs to - so a caller can see where to look rather than parse the message.</summary>
    public EtlImportErrorDetail? ErrorDetail { get; set; }

    /// <summary>What a purge removed. Only set on documents whose status is Purged; a purge is not
    /// a run, so it has no stages, datasets or output keys.</summary>
    public EtlImportPurgeDocument? Purge { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class EtlImportPurgeDocument
{
    /// <summary>The stage names the purge resolved to, downstream cascade included.</summary>
    public List<string> Stages { get; set; } = [];

    /// <summary>Objects deleted across every stage.</summary>
    public int DeletedCount { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class EtlImportViewTableDocument
{
    public required string Name { get; set; }
    public long RowCount { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class EtlImportStageDocument
{
    public required string Name { get; set; }
    public int ItemCount { get; set; }
    public long ElapsedMs { get; set; }
    public DateTime CompletedAtUtc { get; set; }
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class EtlImportDatasetDocument
{
    public required string Dataset { get; set; }

    public List<EtlImportSourceFileDocument> SourceFiles { get; set; } = [];
    public List<string> RawKeys { get; set; } = [];
    public List<string> NormalisedKeys { get; set; } = [];
    public List<string> OptimisedKeys { get; set; } = [];

    public string? SnapshotKey { get; set; }

    /// <summary>The newest source timestamp folded into the snapshot. Not the time the ETL ran.</summary>
    public DateTime? SnapshotSourceTimestampUtc { get; set; }

    public long? RowCount { get; set; }
    public long? RowsUpserted { get; set; }
    public long? RowsDeleted { get; set; }
    public long? RowsIgnoredDeletes { get; set; }

    /// <summary>Rows whose change type was unrecognised, so they were not applied.</summary>
    public long? RowsRejected { get; set; }

    /// <summary>Columns held by the snapshot that a file applied did not carry. Present so schema drift
    /// is visible to whoever is reading the run, rather than only in the logs.</summary>
    public List<string> ColumnsNullified { get; set; } = [];

    /// <summary>Columns a file introduced that no earlier file carried.</summary>
    public List<string> ColumnsAdded { get; set; } = [];
}

[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class EtlImportSourceFileDocument
{
    public required string Key { get; set; }
    public long Size { get; set; }
}
