using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Pipeline;

/// <summary>Default executor. Runs each stage to completion before the next (the macro checkpoint
/// boundary), while the stage itself streams its items internally. Owns the cross-cutting concerns
/// (timing, logging).</summary>
public sealed class PipelineExecutor(ILogger<PipelineExecutor> logger) : IPipelineExecutor
{
    public async Task RunAsync(PipelineDefinition pipeline, IPipelineContext context, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        logger.LogInformation(
            "Pipeline starting with {StageCount} stage(s): {Stages}",
            pipeline.Steps.Count,
            string.Join(" -> ", pipeline.StageNames));

        try
        {
            // Materialise the source before the first stage so each stage sees a fully-drained input
            // (the macro checkpoint boundary), while the stage itself streams internally.
            var current = await DrainAsync(pipeline.Source(context, cancellationToken), cancellationToken);
            logger.LogDebug("Source produced {ItemCount} item(s)", current.Count);

            foreach (var step in pipeline.Steps)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var stageStopwatch = Stopwatch.StartNew();
                var output = step.Invoke(ToStream(current, cancellationToken), context, cancellationToken);
                current = await DrainAsync(output, cancellationToken);
                stageStopwatch.Stop();

                logger.LogInformation(
                    "Stage '{Stage}' completed in {ElapsedMs}ms, produced {ItemCount} item(s)",
                    step.Name,
                    stageStopwatch.ElapsedMilliseconds,
                    current.Count);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Pipeline cancelled after {ElapsedMs}ms", stopwatch.ElapsedMilliseconds);
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Pipeline failed after {ElapsedMs}ms", stopwatch.ElapsedMilliseconds);
            throw;
        }

        stopwatch.Stop();
        logger.LogInformation(
            "Pipeline completed {StageCount} stage(s) in {ElapsedMs}ms",
            pipeline.Steps.Count,
            stopwatch.ElapsedMilliseconds);
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
