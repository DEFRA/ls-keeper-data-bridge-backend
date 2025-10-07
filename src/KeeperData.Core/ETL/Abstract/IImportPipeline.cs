namespace KeeperData.Core.ETL.Abstract;

public interface IImportPipeline
{
    Task StartAsync(Guid importId, string sourceType, CancellationToken ct);
}