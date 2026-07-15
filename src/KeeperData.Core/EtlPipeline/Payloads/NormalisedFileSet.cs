using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>A dataset's files converted to Parquet in normalised/. Output of normalise, input to snapshot.</summary>
public sealed record NormalisedFileSet(DataSetDefinition Definition)
{
    public Guid RunId { get; init; }

    /* Delete this region once the previous stage provides these */
    #region TEMP - PlaceholderInputs

    public IReadOnlyList<string> Files { get; init; } = [];

    #endregion
}
