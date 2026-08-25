using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>One file found in the source, matched to the dataset it belongs to.
/// Emitted by the source stage; not yet grouped or opened.</summary>
public sealed record DiscoveredFile(DataSetDefinition Definition, EtlFile File);
