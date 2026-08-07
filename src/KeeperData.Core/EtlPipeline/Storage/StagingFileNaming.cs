using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>Naming convention for the files in the <see cref="EtlPipelineFolders.Staging"/> folder.
/// Keys are relative to that folder, because <see cref="IEtlPipelineStorageProvider.ForFolder"/> hands
/// out storage already rooted at it.</summary>
public static class StagingFileNaming
{
    public const string DatabasePrefix = "keeper_data_bridge_";

    public const string DatabaseExtension = ".duckdb";

    public const string DatabaseContentType = "application/octet-stream";

    /// <summary>The key of the staging database holding snapshots up to a given source timestamp,
    /// e.g. <c>keeper_data_bridge_20251115121333.duckdb</c>.</summary>
    public static string DatabaseKey(DateTimeOffset sourceTimestamp, string dateTimePattern = EtlConstants.DateTimePattern)
        => $"{DatabasePrefix}{sourceTimestamp.UtcDateTime.ToString(dateTimePattern)}{DatabaseExtension}";
}
