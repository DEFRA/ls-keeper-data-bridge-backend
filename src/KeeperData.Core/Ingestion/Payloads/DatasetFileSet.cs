using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Payloads;

/// <summary>A dataset's main/seed file plus its ordered deltas - discovered, NOT opened.</summary>
public sealed record DatasetFileSet(
    DataSetDefinition Dataset,
    DiscoveredFile Main,
    IReadOnlyList<DiscoveredFile> Deltas);
