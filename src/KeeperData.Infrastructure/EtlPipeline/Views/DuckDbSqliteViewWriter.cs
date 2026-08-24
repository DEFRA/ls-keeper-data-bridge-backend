using DuckDB.NET.Data;
using KeeperData.Core.EtlPipeline.Views;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.EtlPipeline.Views;

/// <summary>Runs the transformation in DuckDB, writing into an attached SQLite database.
///
/// The main catalogue is in-memory and the staging database is attached read-only, so the file the
/// pipeline downloaded cannot be written to even by accident - DuckDB rewrites a database's header on
/// open otherwise, and "the source is untouched" would be a convention rather than a guarantee.
///
/// The whole script runs as one command. DuckDB prepares and executes its statements in order, and
/// the temporary macros and views it defines stay in scope for the statements that follow.</summary>
public sealed class DuckDbSqliteViewWriter(
    IOptions<DuckDbConfiguration> configuration,
    ILogger<DuckDbSqliteViewWriter> logger) : ISqliteViewWriter
{
    private readonly DuckDbConfiguration _configuration = configuration.Value;

    public async Task<SqliteViewWriteResult> WriteAsync(
        SqliteViewWriteRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (File.Exists(request.TargetDatabasePath))
        {
            throw new InvalidOperationException($"SQLite view '{request.TargetDatabasePath}' already exists");
        }

        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync(cancellationToken);

        await LoadSqliteExtensionAsync(connection, cancellationToken);
        await ApplyLimitsAsync(connection, request.TargetDatabasePath, cancellationToken);

        await ExecuteAsync(connection, $"ATTACH {Literal(request.SourceDatabasePath)} AS source (READ_ONLY)", cancellationToken);
        await ExecuteAsync(connection, $"ATTACH {Literal(request.TargetDatabasePath)} AS target (TYPE sqlite)", cancellationToken);
        await ExecuteAsync(connection, "USE source", cancellationToken);

        await ExecuteAsync(connection, request.Sql, cancellationToken);

        var tables = new List<SqliteViewTable>(request.TableNames.Count);

        foreach (var name in request.TableNames)
        {
            var rowCount = await ScalarAsync(connection, $"SELECT count(*) FROM target.{Identifier(name)}", cancellationToken);

            logger.LogInformation("SQLite view table {TableName} holds {RowCount} row(s)", name, rowCount);

            tables.Add(new SqliteViewTable(name, rowCount));
        }

        await ExecuteAsync(connection, "CHECKPOINT target", cancellationToken);
        await ExecuteAsync(connection, "DETACH target", cancellationToken);

        return new SqliteViewWriteResult(tables);
    }

    private async Task LoadSqliteExtensionAsync(DuckDBConnection connection, CancellationToken cancellationToken)
    {
        var path = _configuration.SqliteExtensionPath;

        // Autoloading reaches the network, which the task cannot do; turning it off means a missing
        // bundled extension fails here rather than as a download timeout further in.
        await ExecuteAsync(
            connection,
            "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false;",
            cancellationToken);

        try
        {
            await ExecuteAsync(connection, $"LOAD {Literal(path)}", cancellationToken);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Could not load the DuckDB SQLite extension from {ExtensionPath}", path);

            throw new SqliteViewExtensionException(exception);
        }
    }

    private async Task ApplyLimitsAsync(
        DuckDBConnection connection,
        string targetDatabasePath,
        CancellationToken cancellationToken)
    {
        // Spilling belongs beside the output, on the volume the run was sized for, not wherever
        // DuckDB would otherwise choose.
        var workingDirectory = Path.GetDirectoryName(targetDatabasePath);

        if (!string.IsNullOrEmpty(workingDirectory))
        {
            await ExecuteAsync(
                connection,
                $"SET temp_directory={Literal(Path.Combine(workingDirectory, "duckdb-tmp"))}",
                cancellationToken);
        }

        if (!string.IsNullOrWhiteSpace(_configuration.MemoryLimit))
        {
            await ExecuteAsync(connection, $"SET memory_limit={Literal(_configuration.MemoryLimit)}", cancellationToken);
        }
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

    /// <summary>Table names come from the transformation definition rather than from file content,
    /// but they still reach SQL as text, so they are quoted rather than interpolated bare.</summary>
    private static string Identifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Table name is required", nameof(name));
        }

        return $"\"{name.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    }

    private static string Literal(string value)
        => $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";
}
