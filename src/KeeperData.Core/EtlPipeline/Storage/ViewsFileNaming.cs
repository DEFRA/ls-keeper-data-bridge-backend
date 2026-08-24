using KeeperData.Core.ETL.Impl;
using System.Globalization;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>Naming convention for the files in the <see cref="EtlPipelineFolders.Views"/> folder.
/// Keys are relative to that folder, because <see cref="IEtlPipelineStorageProvider.ForFolder"/> hands
/// out storage already rooted at it.</summary>
public static class ViewsFileNaming
{
    /// <summary>Deliberately generic: this database is expected to carry more than the SAM read model
    /// over time. Also distinct from the legacy <c>cphs_</c> export, which shares this folder.</summary>
    public const string DatabasePrefix = "krds-db_";

    public const string DatabaseExtension = ".sqlite";

    public const string DatabaseContentType = "application/x-sqlite3";

    /// <summary>Identifies which build of the transformation produced an object, so a changed script
    /// rebuilds rather than being skipped as already present.</summary>
    public const string VersionMetadataKey = "krds-view-version";

    public const string TableCountMetadataPrefix = "krds-view-count-";

    /// <summary>The key of the SQLite read model built from the staging database for a given source
    /// timestamp, e.g. <c>krds-db_20260821070003.sqlite</c>.</summary>
    public static string DatabaseKey(DateTimeOffset sourceTimestamp, string dateTimePattern = EtlConstants.DateTimePattern)
        => $"{DatabasePrefix}{sourceTimestamp.UtcDateTime.ToString(dateTimePattern)}{DatabaseExtension}";

    /// <summary>Whether a key belongs to this folder's SQLite exports rather than to another
    /// producer writing into views/.</summary>
    public static bool IsDatabaseKey(string key)
    {
        if (!key.StartsWith(DatabasePrefix, StringComparison.Ordinal)
            || !key.EndsWith(DatabaseExtension, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var timestamp = key[DatabasePrefix.Length..^DatabaseExtension.Length];

        return DateTime.TryParseExact(
            timestamp,
            EtlConstants.DateTimePattern,
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out _);
    }

    public static string TableCountMetadataKey(string tableName)
        => $"{TableCountMetadataPrefix}{tableName.ToLowerInvariant()}";
}
