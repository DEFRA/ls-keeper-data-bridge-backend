using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Etl.Tool.Configuration;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Text.Json;

namespace KeeperData.Etl.Tool.Status;

/// <summary>SQLite-backed import status, standing in for the Mongo store on a desktop run.
///
/// The document is kept as JSON with the few fields the queries need promoted to columns. Shredding
/// the nested stage and dataset entries into tables would buy nothing - nothing filters on them -
/// and would be a second place for the shape to drift from <see cref="EtlImportDocument"/>.
///
/// One run writes its own row and nothing else writes it, so progress is read-merge-replace, and
/// <see cref="EtlImportProgress"/> owns the merge so this store and the Mongo one cannot disagree.</summary>
internal sealed class SqliteEtlImportStatusStore : IEtlImportStatusStore
{
    private static readonly JsonSerializerOptions s_json = new() { WriteIndented = false };

    private readonly string _connectionString;
    private readonly TimeProvider _timeProvider;

    public SqliteEtlImportStatusStore(IOptions<EtlToolOptions> options, TimeProvider timeProvider)
    {
        var path = options.Value.HistoryDatabasePath;

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        _connectionString = new SqliteConnectionStringBuilder { DataSource = path }.ToString();
        _timeProvider = timeProvider;

        EnsureSchema();
    }

    public Task CreateQueuedAsync(Guid importId, string sourceType, string? dataset, CancellationToken cancellationToken)
        => UpsertAsync(EtlImportProgress.Queued(importId, sourceType, dataset, UtcNow), cancellationToken);

    public async Task MarkRunningAsync(Guid importId, IReadOnlyList<string> stageNames, CancellationToken cancellationToken)
    {
        var document = await LoadAsync(importId, cancellationToken);

        if (document is null) return;

        var now = UtcNow;

        document.Status = EtlImportStatus.Running.ToString();
        document.StartedAtUtc = now;
        document.CurrentStage = stageNames.Count > 0 ? stageNames[0] : null;
        document.LeaseExpiresAtUtc = now.Add(EtlImportProgress.LeaseDuration);

        await UpsertAsync(document, cancellationToken);
    }

    public async Task MarkStageRunningAsync(Guid importId, string stageName, CancellationToken cancellationToken)
    {
        var document = await LoadAsync(importId, cancellationToken);

        if (document is null) return;

        document.CurrentStage = stageName;
        document.LeaseExpiresAtUtc = UtcNow.Add(EtlImportProgress.LeaseDuration);

        await UpsertAsync(document, cancellationToken);
    }

    public async Task RecordStageAsync(Guid importId, EtlImportStageProgress progress, CancellationToken cancellationToken)
    {
        var document = await LoadAsync(importId, cancellationToken);

        if (document is null) return;

        EtlImportProgress.ApplyStage(document, progress, UtcNow);

        await UpsertAsync(document, cancellationToken);
    }

    public Task MarkSucceededAsync(Guid importId, CancellationToken cancellationToken)
        => CompleteAsync(importId, EtlImportStatus.Succeeded, error: null, detail: null, cancellationToken);

    public Task MarkFailedAsync(Guid importId, string error, EtlImportErrorDetail? detail, CancellationToken cancellationToken)
        => CompleteAsync(importId, EtlImportStatus.Failed, error, detail, cancellationToken);

    public async Task<EtlImportDocument?> GetAsync(Guid importId, CancellationToken cancellationToken)
    {
        var document = await LoadAsync(importId, cancellationToken);

        return document is null ? null : EtlImportProgress.AsAbandonedIfLapsed(document, UtcNow);
    }

    public async Task<EtlImportDocument?> GetInFlightAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);

        var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT Document FROM EtlImport
            WHERE Status IN ('Queued', 'Running')
            ORDER BY RequestedAtUtc DESC
            LIMIT 10
            """;

        var now = UtcNow;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var document = Deserialise(reader.GetString(0));

            if (EtlImportProgress.IsInFlight(document, now)) return document;
        }

        return null;
    }

    public async Task<EtlImportPage> ListAsync(int skip, int top, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);

        var page = connection.CreateCommand();
        page.CommandText =
            """
            SELECT Document FROM EtlImport
            ORDER BY RequestedAtUtc DESC
            LIMIT $top OFFSET $skip
            """;
        page.Parameters.AddWithValue("$top", top);
        page.Parameters.AddWithValue("$skip", skip);

        var now = UtcNow;
        var documents = new List<EtlImportDocument>();

        await using (var reader = await page.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                documents.Add(EtlImportProgress.AsAbandonedIfLapsed(Deserialise(reader.GetString(0)), now));
            }
        }

        var count = connection.CreateCommand();
        count.CommandText = "SELECT count(*) FROM EtlImport";

        var total = Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);

        return new EtlImportPage(documents, total);
    }

    public Task RecordPurgeAsync(EtlPurgeRecord purge, CancellationToken cancellationToken)
        => UpsertAsync(EtlImportProgress.Purged(purge, UtcNow), cancellationToken);

    private async Task CompleteAsync(
        Guid importId,
        EtlImportStatus status,
        string? error,
        EtlImportErrorDetail? detail,
        CancellationToken cancellationToken)
    {
        var document = await LoadAsync(importId, cancellationToken);

        if (document is null) return;

        EtlImportProgress.Complete(document, status, error, detail, UtcNow);

        await UpsertAsync(document, cancellationToken);
    }

    private async Task<EtlImportDocument?> LoadAsync(Guid importId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);

        var command = connection.CreateCommand();
        command.CommandText = "SELECT Document FROM EtlImport WHERE ImportId = $id";
        command.Parameters.AddWithValue("$id", importId.ToString());

        var json = await command.ExecuteScalarAsync(cancellationToken) as string;

        return json is null ? null : Deserialise(json);
    }

    private async Task UpsertAsync(EtlImportDocument document, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);

        var command = connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO EtlImport (ImportId, Status, SourceType, Dataset, RequestedAtUtc, LeaseExpiresAtUtc, Document)
            VALUES ($id, $status, $sourceType, $dataset, $requested, $lease, $document)
            ON CONFLICT (ImportId) DO UPDATE SET
                Status = excluded.Status,
                SourceType = excluded.SourceType,
                Dataset = excluded.Dataset,
                RequestedAtUtc = excluded.RequestedAtUtc,
                LeaseExpiresAtUtc = excluded.LeaseExpiresAtUtc,
                Document = excluded.Document
            """;

        command.Parameters.AddWithValue("$id", document.ImportId.ToString());
        command.Parameters.AddWithValue("$status", document.Status);
        command.Parameters.AddWithValue("$sourceType", document.SourceType);
        command.Parameters.AddWithValue("$dataset", (object?)document.Dataset ?? DBNull.Value);
        command.Parameters.AddWithValue("$requested", Text(document.RequestedAtUtc));
        command.Parameters.AddWithValue("$lease", document.LeaseExpiresAtUtc is { } lease ? Text(lease) : DBNull.Value);
        command.Parameters.AddWithValue("$document", JsonSerializer.Serialize(document, s_json));

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private void EnsureSchema()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        // RequestedAtUtc is text so the sort is the chronological one: round-trip format sorts
        // lexicographically, which a local-format string would not.
        command.CommandText =
            """
            CREATE TABLE IF NOT EXISTS EtlImport (
                ImportId TEXT PRIMARY KEY,
                Status TEXT NOT NULL,
                SourceType TEXT NOT NULL,
                Dataset TEXT NULL,
                RequestedAtUtc TEXT NOT NULL,
                LeaseExpiresAtUtc TEXT NULL,
                Document TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_etl_import_requested ON EtlImport (RequestedAtUtc DESC);
            CREATE INDEX IF NOT EXISTS ix_etl_import_status ON EtlImport (Status);
            """;

        command.ExecuteNonQuery();
    }

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static EtlImportDocument Deserialise(string json)
        => JsonSerializer.Deserialize<EtlImportDocument>(json, s_json)
           ?? throw new InvalidOperationException("An import row held JSON that is not an import document.");

    private static string Text(DateTime value)
        => value.ToString("O", CultureInfo.InvariantCulture);

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;
}
