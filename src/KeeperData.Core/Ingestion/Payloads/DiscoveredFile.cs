using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Payloads;

/// <summary>One object found in the external source, already matched to a dataset and labelled.</summary>
public sealed record DiscoveredFile(
    DataSetDefinition Dataset,
    string SourceKey,
    DateTimeOffset Timestamp,
    bool IsDelta);
