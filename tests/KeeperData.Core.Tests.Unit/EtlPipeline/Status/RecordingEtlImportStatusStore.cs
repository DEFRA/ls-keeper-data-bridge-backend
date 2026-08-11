using KeeperData.Core.EtlPipeline.Status;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Status;

/// <summary>In-memory stand-in for the Mongo store: records the calls so a test can assert what the
/// observer derived from a run, without a database.</summary>
public sealed class RecordingEtlImportStatusStore : IEtlImportStatusStore
{
    public List<(Guid ImportId, string SourceType, string? Dataset)> Queued { get; } = [];
    public List<(Guid ImportId, IReadOnlyList<string> Stages)> Started { get; } = [];
    public List<(Guid ImportId, EtlImportStageProgress Progress)> Progress { get; } = [];
    public List<Guid> Succeeded { get; } = [];
    public List<(Guid ImportId, string Error)> Failed { get; } = [];

    public EtlImportDocument? Document { get; set; }
    public EtlImportDocument? InFlight { get; set; }
    public List<EtlImportDocument> Listed { get; } = [];
    public List<(int Skip, int Top)> ListRequests { get; } = [];

    public Task CreateQueuedAsync(Guid importId, string sourceType, string? dataset, CancellationToken cancellationToken)
    {
        Queued.Add((importId, sourceType, dataset));
        return Task.CompletedTask;
    }

    public Task MarkRunningAsync(Guid importId, IReadOnlyList<string> stageNames, CancellationToken cancellationToken)
    {
        Started.Add((importId, stageNames));
        return Task.CompletedTask;
    }

    public Task RecordStageAsync(Guid importId, EtlImportStageProgress progress, CancellationToken cancellationToken)
    {
        Progress.Add((importId, progress));
        return Task.CompletedTask;
    }

    public Task MarkSucceededAsync(Guid importId, CancellationToken cancellationToken)
    {
        Succeeded.Add(importId);
        return Task.CompletedTask;
    }

    public Task MarkFailedAsync(Guid importId, string error, CancellationToken cancellationToken)
    {
        Failed.Add((importId, error));
        return Task.CompletedTask;
    }

    public Task<EtlImportDocument?> GetAsync(Guid importId, CancellationToken cancellationToken)
        => Task.FromResult(Document);

    public Task<EtlImportDocument?> GetInFlightAsync(CancellationToken cancellationToken)
        => Task.FromResult(InFlight);

    public Task<EtlImportPage> ListAsync(int skip, int top, CancellationToken cancellationToken)
    {
        ListRequests.Add((skip, top));
        return Task.FromResult(new EtlImportPage([.. Listed.Skip(skip).Take(top)], Listed.Count));
    }
}
