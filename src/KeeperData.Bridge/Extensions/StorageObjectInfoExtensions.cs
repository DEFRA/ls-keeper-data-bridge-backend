using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Bridge.Extensions
{
    public static class StorageObjectInfoExtensions
    {
        public static StorageObjectInfo? GetLatest(this IReadOnlyList<StorageObjectInfo> objects) => 
            objects.Where(o => o.Key.Contains(StagingFileNaming.DatabasePrefix, StringComparison.Ordinal) 
            && o.Key.EndsWith(StagingFileNaming.DatabaseExtension, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(o => o.Key, StringComparer.Ordinal)
            .FirstOrDefault();

        /// <summary>The newest SQLite read model. Prefix-scoped rather than extension-scoped, because
        /// views/ also holds the legacy cphs_ export.</summary>
        public static StorageObjectInfo? GetLatestSqliteView(this IReadOnlyList<StorageObjectInfo> objects) =>
            objects.Where(o => ViewsFileNaming.IsDatabaseKey(o.Key))
            .OrderByDescending(o => o.Key, StringComparer.Ordinal)
            .FirstOrDefault();
    }
}