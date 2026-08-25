using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Pipeline;

/// <summary>Default executor. Runs each stage to completion before the next (the macro checkpoint
/// boundary), while the stage itself streams its items internally. Owns the cross-cutting concerns
/// (timing, logging, notifying observers).</summary>
public sealed class PipelineExecutor(
    ILogger<PipelineExecutor> logger,
    IEnumerable<IPipelineRunObserver>? observers = null) : IPipelineExecutor
{
    private readonly IReadOnlyList<IPipelineRunObserver> _observers = observers?.ToArray() ?? [];

    public async Task RunAsync(PipelineDefinition pipeline, IPipelineContext context, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var stageNames = pipeline.GetStageNames();

        logger.LogInformation(
            "Pipeline starting with {StageCount} stage(s): {Stages}",
            pipeline.Steps.Count,
            string.Join(" -> ", stageNames));

        await NotifyAsync(o => o.RunStartingAsync(context, stageNames, cancellationToken));

        try
        {
            // Materialise the source before the first stage so each stage sees a fully-drained input
            // (the macro checkpoint boundary), while the stage itself streams internally.
            var current = await DrainAsync(pipeline.Source(context, cancellationToken), cancellationToken);
            logger.LogDebug("Source produced {ItemCount} item(s)", current.Count);

            foreach (var step in pipeline.Steps)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await NotifyAsync(o => o.StageStartingAsync(context, step.Name, cancellationToken));

                var stageStopwatch = Stopwatch.StartNew();
                var output = step.Invoke(ToStream(current, cancellationToken), context, cancellationToken);
                current = await DrainAsync(output, cancellationToken);
                stageStopwatch.Stop();

                logger.LogInformation(
                    "Stage '{Stage}' completed in {ElapsedMs}ms, produced {ItemCount} item(s)",
                    step.Name,
                    stageStopwatch.ElapsedMilliseconds,
                    current.Count);

                await NotifyAsync(o => o.StageCompletedAsync(context, step.Name, current, stageStopwatch.Elapsed, cancellationToken));
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Pipeline cancelled after {ElapsedMs}ms", stopwatch.ElapsedMilliseconds);
            throw;
        }
        catch (Exception ex)
        {
            var failure = new PipelineExecutionException(
                $"Pipeline failed after {stopwatch.ElapsedMilliseconds}ms.", ex);

            await NotifyAsync(o => o.RunFailedAsync(context, failure, CancellationToken.None));

            throw failure;
        }

        stopwatch.Stop();
        logger.LogInformation(
            "Pipeline completed {StageCount} stage(s) in {ElapsedMs}ms",
            pipeline.Steps.Count,
            stopwatch.ElapsedMilliseconds);

        await NotifyAsync(o => o.RunCompletedAsync(context, stopwatch.Elapsed, cancellationToken));
    }

    // Observers report status; they must never be able to fail the run they are reporting on.
    private async Task NotifyAsync(Func<IPipelineRunObserver, Task> notify)
    {
        foreach (var observer in _observers)
        {
            try
            {
                await notify(observer);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Pipeline run observer {Observer} threw and was ignored", observer.GetType().Name);
            }
        }
    }

    private static async Task<IReadOnlyList<object>> DrainAsync(IAsyncEnumerable<object> source, CancellationToken cancellationToken)
    {
        var items = new List<object>();
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            items.Add(item);
        }
        return items;
    }

    private static async IAsyncEnumerable<object> ToStream(IReadOnlyList<object> items, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
        await Task.CompletedTask;
    }
}

public class PipelineExecutionException : Exception
{
    public PipelineExecutionException(string message, Exception exception) : base(message, exception)
    {
    }
}
