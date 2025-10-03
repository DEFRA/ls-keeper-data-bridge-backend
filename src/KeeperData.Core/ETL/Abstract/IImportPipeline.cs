namespace KeeperData.Core.ETL.Abstract;

public interface IImportPipeline
{
    Task StartAsync(string sourceType, CancellationToken ct);
}