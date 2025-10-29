namespace KeeperData.Core.ETL.Abstract;

public interface IIngestionPipeline
{
    Task StartAsync(Guid importId, CancellationToken ct);
}