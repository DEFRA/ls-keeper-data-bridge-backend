namespace KeeperData.Core.Pipeline;

/// <summary>Watches a run without taking part in it.
///
/// The executor already drains every stage's output; an observer is handed those items so run
/// status, metrics or events can be derived from them without a stage knowing anything about where
/// its results are reported. Observers are advisory: an observer that throws is logged and ignored,
/// never allowed to fail the run.</summary>
public interface IPipelineRunObserver
{
    Task RunStartingAsync(
        IPipelineContext context,
        IReadOnlyList<string> stageNames,
        CancellationToken cancellationToken);

    Task StageCompletedAsync(
        IPipelineContext context,
        string stageName,
        IReadOnlyList<object> items,
        TimeSpan elapsed,
        CancellationToken cancellationToken);

    Task RunCompletedAsync(
        IPipelineContext context,
        TimeSpan elapsed,
        CancellationToken cancellationToken);

    Task RunFailedAsync(
        IPipelineContext context,
        Exception exception,
        CancellationToken cancellationToken);
}
