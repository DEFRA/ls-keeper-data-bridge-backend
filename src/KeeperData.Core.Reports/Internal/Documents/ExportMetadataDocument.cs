using System.Diagnostics.CodeAnalysis;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reports.Internal.Documents;

/// <summary>
/// Singleton document that tracks the timestamp of the last successful incremental export.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Internal persistence document - covered by integration tests.")]
internal class ExportMetadataDocument
{
    [BsonId] public string Id { get; set; } = "singleton";
    [BsonElement("last_exported_at_utc")] public DateTime? LastExportedAtUtc { get; set; }
}
