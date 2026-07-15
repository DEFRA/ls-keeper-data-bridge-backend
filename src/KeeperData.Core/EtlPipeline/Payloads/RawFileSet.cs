using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>A dataset's files decrypted into raw/. Output of decrypt, input to normalise.</summary>
public sealed record RawFileSet(DataSetDefinition Definition)
{
    public Guid RunId { get; init; }

    /* Delete this region once the previous stage provides these */
    #region TEMP - PlaceholderInputs

    public IReadOnlyList<string> Files { get; init; } = [];

    #endregion
}
