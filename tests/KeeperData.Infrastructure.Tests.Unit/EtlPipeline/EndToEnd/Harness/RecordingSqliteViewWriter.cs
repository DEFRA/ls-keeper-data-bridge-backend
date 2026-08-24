using KeeperData.Core.EtlPipeline.Views;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;

/// <summary>
/// Stands in for <c>DuckDbSqliteViewWriter</c>, recording what the export stage asked it to build and
/// producing a file at the target path without a database engine.
///
/// Pass the real writer to <c>InMemoryEtlPipelineHost.Create</c> instead to cover the transformation
/// itself as part of an end-to-end run.
/// </summary>
public sealed class RecordingSqliteViewWriter : ISqliteViewWriter
{
    private readonly List<SqliteViewWriteRequest> _calls = [];

    /// <summary>Every request handed to the writer, in call order.</summary>
    public IReadOnlyList<SqliteViewWriteRequest> Calls => _calls;

    /// <summary>The request from the single call, failing loudly if there was not exactly one.</summary>
    public SqliteViewWriteRequest OnlyCall => _calls.Single();

    public async Task<SqliteViewWriteResult> WriteAsync(
        SqliteViewWriteRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        _calls.Add(request);

        // The export stage uploads whatever is at this path, so it has to exist.
        await File.WriteAllTextAsync(request.TargetDatabasePath, "recorded sqlite view", cancellationToken);

        return new SqliteViewWriteResult([.. request.TableNames.Select(name => new SqliteViewTable(name, 0))]);
    }
}
