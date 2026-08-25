namespace KeeperData.Core.EtlPipeline.Staging;

/// <summary>One dataset's snapshot on local disk, and the table it becomes.</summary>
public sealed record StagingTableSource(string TableName, string ParquetPath, string SnapshotKey);

/// <summary>A table in the staging database, and how many rows it holds.</summary>
public sealed record StagingTable(string Name, string SnapshotKey, long RowCount);

public sealed record StagingDatabaseWriteResult(IReadOnlyList<StagingTable> Tables);

/// <summary>Builds the staging database from snapshot Parquet files.
///
/// Local paths in, local database out: the implementation owns the database engine and knows nothing
/// about object storage, which keeps the engine out of Core and lets the load stage be tested without
/// it.</summary>
public interface IStagingDatabaseWriter
{
    /// <param name="databasePath">Where to create the database. Must not already exist.</param>
    /// <exception cref="InvalidOperationException">A table's row count does not match its Parquet
    /// source, so the database must not be published.</exception>
    Task<StagingDatabaseWriteResult> WriteAsync(
        IReadOnlyList<StagingTableSource> sources,
        string databasePath,
        CancellationToken cancellationToken = default);
}
