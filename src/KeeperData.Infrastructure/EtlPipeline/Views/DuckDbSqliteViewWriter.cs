using DuckDB.NET.Data;
using KeeperData.Core.EtlPipeline.Views;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;

namespace KeeperData.Infrastructure.EtlPipeline.Views;

/// <summary>Runs the transformation in DuckDB, writing into an attached SQLite database.
///
/// The main catalogue is in-memory and the staging database is attached read-only, so the file the
/// pipeline downloaded cannot be written to even by accident - DuckDB rewrites a database's header on
/// open otherwise, and "the source is untouched" would be a convention rather than a guarantee.
///
/// Each part runs as one command. DuckDB prepares and executes its statements in order, and the
/// temporary macros and views it defines stay in scope for the statements that follow. A part whose
/// source system this environment does not load is skipped, so an absent CTS extract costs the read
/// model its Open Locations table rather than all of it.</summary>
public sealed class DuckDbSqliteViewWriter(
    IOptions<DuckDbConfiguration> configuration,
    ILogger<DuckDbSqliteViewWriter> logger) : ISqliteViewWriter
{
    /// <summary>The variable the transformation reads its as-at date from. Set before the script
    /// runs rather than written into it, so that the script stays a constant and its fingerprint
    /// identifies the transformation rather than the run.</summary>
    private const string QueryDateVariable = "cts_query_date";

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

        await SetQueryDateAsync(connection, request.QueryDate, cancellationToken);

        var staged = await StagedTablesAsync(connection, cancellationToken);
        var tables = new List<SqliteViewTable>();

        foreach (var part in request.Parts)
        {
            if (!ShouldRun(part, staged))
            {
                continue;
            }

            await ExecuteAsync(connection, part.Sql, cancellationToken);

            foreach (var name in part.TableNames)
            {
                var rowCount = await ScalarAsync(connection, $"SELECT count(*) FROM target.{Identifier(name)}", cancellationToken);

                logger.LogInformation("SQLite view table {TableName} holds {RowCount} row(s)", name, rowCount);

                tables.Add(new SqliteViewTable(name, rowCount));
            }
        }

        await ExecuteAsync(connection, "CHECKPOINT target", cancellationToken);
        await ExecuteAsync(connection, "DETACH target", cancellationToken);

        return new SqliteViewWriteResult(tables);
    }

    /// <summary>Whether this environment loaded the source system the part reads. All of its tables
    /// or none of them: a part that ran against a partial load would silently produce a partial
    /// answer, which is worse than no answer at all.</summary>
    private bool ShouldRun(SqliteViewPart part, IReadOnlySet<string> staged)
    {
        var missing = part.RequiredSourceTables.Where(table => !staged.Contains(table)).ToArray();

        if (missing.Length == 0)
        {
            return true;
        }

        if (part.Required)
        {
            throw new InvalidOperationException(
                $"The staging database is missing {string.Join(", ", missing)}, which " +
                $"'{part.Name}' reads. The read model cannot be built without it.");
        }

        logger.LogWarning(
            "Skipping {PartName}: the staging database does not hold {MissingTables}. " +
            "The read model will not carry {TableNames}",
            part.Name, string.Join(", ", missing), string.Join(", ", part.TableNames));

        return false;
    }

    /// <summary>The tables the attached staging database actually holds. Compared without case
    /// because the loader quotes the dataset name when it creates them, so their stored case is
    /// whatever the dataset definition carries rather than DuckDB's own folding.</summary>
    private static async Task<IReadOnlySet<string>> StagedTablesAsync(
        DuckDBConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT table_name FROM duckdb_tables() WHERE database_name = 'source'";

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        while (await reader.ReadAsync(cancellationToken))
        {
            tables.Add(reader.GetString(0));
        }

        return tables;
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
        // Physical row order is not part of the read-model contract. Letting DuckDB reorder work
        // avoids retaining order-tracking buffers that otherwise count against the memory limit.
        await ExecuteAsync(connection, "SET preserve_insertion_order=false", cancellationToken);

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

    private static async Task SetQueryDateAsync(
        DuckDBConnection connection,
        DateTimeOffset queryDate,
        CancellationToken cancellationToken)
    {
        // The date part alone: the snapshots carry a time of day, but every rule the transformation
        // applies it to is date-granular, and keeping the time would make an afternoon run of the
        // same snapshots a different query than a morning one.
        var value = queryDate.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        await ExecuteAsync(connection, $"SET VARIABLE {QueryDateVariable} = DATE {Literal(value)}", cancellationToken);
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
