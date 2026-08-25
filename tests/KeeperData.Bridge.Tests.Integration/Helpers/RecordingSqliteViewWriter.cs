using KeeperData.Core.EtlPipeline.Views;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>The LocalStack fixtures do not carry the full SAM schema; the real transformation is
/// covered in the infrastructure tests, while this writer keeps the storage and pipeline wiring real.</summary>
public sealed class RecordingSqliteViewWriter : ISqliteViewWriter
{
    public async Task<SqliteViewWriteResult> WriteAsync(
        SqliteViewWriteRequest request,
        CancellationToken cancellationToken = default)
    {
        await File.WriteAllTextAsync(request.TargetDatabasePath, "recorded sqlite view", cancellationToken);

        return new SqliteViewWriteResult(
            [.. request.TableNames.Select(name => new SqliteViewTable(name, 0))]);
    }
}