using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;

namespace KeeperData.Core.ETL.Abstract;

public interface IIngestionPipeline
{
    Task StartAsync(ImportReport report, CancellationToken ct);
}