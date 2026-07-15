namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>The single DuckDB staging database in staging/. Final output of the pipeline.</summary>
public sealed record StagingDatabase
{
    public Guid RunId { get; init; }

    /* Delete this region once the previous stage provides these */
    #region TEMP - PlaceholderInputs

    public string DatabaseKey { get; init; } = string.Empty;

    #endregion
}
