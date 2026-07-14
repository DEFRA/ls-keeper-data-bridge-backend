using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>Output of the discovery stage: one dataset and the files found for it.</summary>
public sealed record DiscoveredFileSet(DataSetDefinition Definition, IReadOnlyList<EtlFile> Files);
