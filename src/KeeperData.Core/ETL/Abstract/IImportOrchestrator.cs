
namespace KeeperData.Core.ETL.Impl;

public interface IImportOrchestrator
{
    Task StartAsync(Guid importId, string sourceType, CancellationToken ct);
}