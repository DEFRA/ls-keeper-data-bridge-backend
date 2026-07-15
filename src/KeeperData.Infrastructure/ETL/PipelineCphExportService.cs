using KeeperData.Core.ETL.Export;
using KeeperData.Core.ETL.Export.Pipeline;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.ETL.Pipeline;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.ETL;

/// <summary>
/// <see cref="ICphExportService"/> implementation expressed on the generic pipeline framework
/// (<c>DuckDbCphSource -&gt; CphSqliteSink</c>). Behaviour matches <see cref="CphExportService"/>;
/// the executor owns sequencing/timing/logging and the result is surfaced via <see cref="CphExportContext"/>.
/// </summary>
public sealed class PipelineCphExportService : ICphExportService
{
    private readonly IPipelineExecutor _executor;
    private readonly IBlobStorageServiceFactory _storageFactory;
    private readonly ILoggerFactory _loggerFactory;

    public PipelineCphExportService(
        IPipelineExecutor executor,
        IBlobStorageServiceFactory storageFactory,
        ILoggerFactory loggerFactory)
    {
        _executor = executor;
        _storageFactory = storageFactory;
        _loggerFactory = loggerFactory;
    }

    public Task<CphExportResult> ExportAsync(CancellationToken cancellationToken = default)
        => RunPipelineAsync(sourceDuckDbKey: null, cancellationToken);

    public Task<CphExportResult> ExportAsync(string sourceDuckDbKey, CancellationToken cancellationToken = default)
        => RunPipelineAsync(sourceDuckDbKey, cancellationToken);

    private async Task<CphExportResult> RunPipelineAsync(string? sourceDuckDbKey, CancellationToken cancellationToken)
    {
        var source = new DuckDbCphSource(_storageFactory, _loggerFactory.CreateLogger<DuckDbCphSource>());
        var sink = new CphSqliteSink(_storageFactory, _loggerFactory.CreateLogger<CphSqliteSink>());

        var definition = PipelineBuilder
            .InputSource(source)
            .Then(sink)
            .Build();

        var context = new CphExportContext { SourceDuckDbKey = sourceDuckDbKey };
        await _executor.RunAsync(definition, context, cancellationToken);

        return context.Result
            ?? throw new InvalidOperationException("CPH export pipeline completed without producing a result.");
    }
}
