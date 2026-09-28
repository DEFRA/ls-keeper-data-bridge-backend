using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>One parquet file the optimise stage resolved for the snapshot to consume: either the
/// rewritten artefact in optimised/, or the untouched normalised original it passes through when
/// the dataset asked for nothing the stage does.</summary>
public sealed record OptimisedFile(string Folder, string Key);

/// <summary>A dataset's files ready for snapshotting, in whichever folder each resolved to.
/// Output of optimise, input to snapshot.</summary>
public sealed record OptimisedFileSet(DataSetDefinition Definition)
{
    public Guid RunId { get; init; }

    public IReadOnlyList<OptimisedFile> Files { get; init; } = [];
}
