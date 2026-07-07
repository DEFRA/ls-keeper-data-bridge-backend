using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Payloads;

namespace KeeperData.Core.Ingestion.Contracts;

//  SHARED SERVICES - the reuse surface. Both the legacy and new pipelines lean on
//  these (password derivation, discovery, storage) rather than duplicating them.

/// <summary>The pipeline head: lists the external source and yields matched, labelled files.</summary>
public interface IFileSource : ISourceStage<DiscoveredFile> { }
