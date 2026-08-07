using DuckDB.NET.Data;
using KeeperData.Core.EtlPipeline.Staging;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.EtlPipeline.Staging;

/// <summary>Builds the staging database by having DuckDB read the snapshot Parquet files directly.
///
/// Reading Parquet here is not the delta merge the pipeline keeps out of DuckDB - the merge has
/// already happened and produced these snapshots. This is the staging load.
///
/// Each table's row count is checked against its Parquet source before the writer returns, so a
/// truncated read fails the load rather than publishing a database that is quietly short of rows.</summary>
public sealed class DuckDbStagingDatabaseWriter(ILogger<DuckDbStagingDatabaseWriter> logger) : IStagingDatabaseWriter
{
    public async Task<StagingDatabaseWriteResult> WriteAsync(
        IReadOnlyList<StagingTableSource> sources,
        string databasePath,
        CancellationToken cancellationToken = default)
    {
        if (File.Exists(databasePath))
        {
            throw new InvalidOperationException($"Staging database '{databasePath}' already exists");
        }

        await using var connection = new DuckDBConnection($"Data Source={databasePath}");
        await connection.OpenAsync(cancellationToken);

        var tables = new List<StagingTable>(sources.Count);

        foreach (var source in sources)
        {
            tables.Add(await CreateTableAsync(connection, source, cancellationToken));
        }

        return new StagingDatabaseWriteResult(tables);
    }

    private async Task<StagingTable> CreateTableAsync(
        DuckDBConnection connection,
        StagingTableSource source,
        CancellationToken cancellationToken)
    {
        var table = QuoteIdentifier(source.TableName);
        var parquet = QuoteLiteral(source.ParquetPath);

        await ExecuteAsync(
            connection,
            $"CREATE TABLE {table} AS SELECT * FROM read_parquet({parquet})",
            cancellationToken);

        var rowCount = await ScalarAsync(connection, $"SELECT count(*) FROM {table}", cancellationToken);
        var parquetRowCount = await ScalarAsync(
            connection, $"SELECT count(*) FROM read_parquet({parquet})", cancellationToken);

        if (rowCount != parquetRowCount)
        {
            throw new InvalidOperationException(
                $"Table '{source.TableName}' holds {rowCount} row(s) but '{source.SnapshotKey}' holds {parquetRowCount}");
        }

        logger.LogInformation(
            "Loaded {RowCount} row(s) from {SnapshotKey} into staging table {TableName}",
            rowCount, source.SnapshotKey, source.TableName);

        return new StagingTable(source.TableName, source.SnapshotKey, rowCount);
    }

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<long> ScalarAsync(DuckDBConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
    }

    /// <summary>Table names come from the dataset definitions rather than from file content, but they
    /// still reach SQL as text, so they are quoted rather than interpolated bare.</summary>
    private static string QuoteIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Table name is required", nameof(name));
        }

        return $"\"{name.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    }

    private static string QuoteLiteral(string value)
        => $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";
}
