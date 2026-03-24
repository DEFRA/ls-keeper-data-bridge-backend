using KeeperData.Infrastructure.Benchmarking.Metrics;
using KeeperData.Infrastructure.Benchmarking.Models;
using KeeperData.Infrastructure.Benchmarking.Throttling;
using System.Diagnostics;

namespace KeeperData.Infrastructure.Benchmarking.Scenarios;

/// <summary>
/// Base class that runs an operation loop for the configured duration,
/// collects per-operation latency, throttles without skewing latency stats,
/// and builds a <see cref="ScenarioResult"/>.
/// Supports parallel workers via <see cref="BenchmarkConfig.Concurrency"/>.
/// </summary>
public abstract class ScenarioBase : IBenchmarkScenario
{
    public abstract string Name { get; }

    /// <summary>Execute a single operation. Return true if successful.</summary>
    protected abstract Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct);

    public async Task<ScenarioResult> RunAsync(BenchmarkConfig config, IBenchmarkThrottler throttler, CancellationToken ct)
    {
        var state = new WorkerState();
        var overallSw = Stopwatch.StartNew();

        var workers = new Task[config.Concurrency];
        for (var w = 0; w < config.Concurrency; w++)
        {
            workers[w] = RunWorkerAsync(state, config.Duration, config.ThrottleDelay, throttler, overallSw, ct);
        }

        try
        {
            await Task.WhenAll(workers);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // expected
        }

        overallSw.Stop();
        return state.ToResult(Name, overallSw.Elapsed);
    }

    private async Task RunWorkerAsync(
        WorkerState state,
        TimeSpan duration,
        TimeSpan throttleDelay,
        IBenchmarkThrottler throttler,
        Stopwatch overallSw,
        CancellationToken ct)
    {
        while (overallSw.Elapsed < duration && !ct.IsCancellationRequested)
        {
            var iteration = state.NextIteration();
            await ExecuteAndRecordAsync(state, iteration, ct);

            try
            {
                await throttler.DelayAsync(throttleDelay, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task ExecuteAndRecordAsync(WorkerState state, int iteration, CancellationToken ct)
    {
        var opSw = Stopwatch.StartNew();
        try
        {
            var ok = await ExecuteOperationAsync(iteration, ct);
            if (!ok) state.IncrementErrors();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // swallow; the worker loop will exit on the next check
        }
        catch
        {
            state.IncrementErrors();
        }
        finally
        {
            opSw.Stop();
            state.RecordLatency(opSw.Elapsed);
        }
    }

    /// <summary>Thread-safe accumulator for scenario worker state.</summary>
    private sealed class WorkerState
    {
        private readonly LatencyRecorder _recorder = new();
        private int _ops;
        private int _errors;
        private double _cumulativeOperationMs;

        public int NextIteration() => Interlocked.Increment(ref _ops) - 1;

        public void IncrementErrors() => Interlocked.Increment(ref _errors);

        public void RecordLatency(TimeSpan elapsed)
        {
            _recorder.Record(elapsed);

            double opMs = elapsed.TotalMilliseconds;
            double initial, computed;
            do
            {
                initial = Volatile.Read(ref _cumulativeOperationMs);
                computed = initial + opMs;
            } while (Interlocked.CompareExchange(ref _cumulativeOperationMs, computed, initial) != initial);
        }

        public ScenarioResult ToResult(string scenarioName, TimeSpan wallTime)
        {
            var totalOps = _ops;
            var elapsed = wallTime.TotalSeconds;
            var cumulativeOpSec = Volatile.Read(ref _cumulativeOperationMs) / 1000.0;

            return new ScenarioResult
            {
                ScenarioName = scenarioName,
                TotalOperations = totalOps,
                ErrorCount = _errors,
                ElapsedSeconds = Math.Round(elapsed, 2),
                OpsPerSecond = elapsed > 0 ? Math.Round(totalOps / elapsed, 2) : 0,
                EffectiveOpsPerSecond = cumulativeOpSec > 0 ? Math.Round(totalOps / cumulativeOpSec, 2) : 0,
                Latency = _recorder.Compute()
            };
        }
    }
}
