namespace KeeperData.Core.ETL.Abstract;

public interface IAcquisitionPipeline
{
    Task StartAsync(Guid importId, string sourceType, CancellationToken ct);
}