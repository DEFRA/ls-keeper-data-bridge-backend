using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Core.EtlPipeline.Concurrency;

/// <summary>The ETL's concurrency budget, and the fan-out that spends it.
///
/// One instance per process, so the budget is a property of the host rather than of a stage: the
/// stages hand their work here and the limit holds however many of them are fanning out at once.
///
/// Budgets are derived rather than configured, because the same image runs on a workstation and on
/// a container with a fraction of a host's cores. <see cref="Environment.ProcessorCount"/> honours
/// the cgroup CPU quota, so it reports what the process may actually use.
///
/// The ETL shares its process with the web host that answers the platform's health probe, so the
/// CPU budget deliberately leaves a core free and the thread pool is given headroom to match. The
/// pool grows by roughly one thread a second once saturated, which is slow enough that a probe
/// queued behind a saturating fan-out fails before it is ever served.</summary>
public sealed class EtlConcurrency : IDisposable
{
    private readonly SemaphoreSlim _cpu;
    private readonly SemaphoreSlim _io;

    public EtlConcurrency(IOptions<EtlConcurrencyOptions> options, ILogger<EtlConcurrency> logger)
    {
        var settings = options.Value;
        var cores = Environment.ProcessorCount;

        CpuBudget = Math.Max(1, settings.CpuBudget ?? cores - settings.ReservedCores);

        // Never below the CPU budget: I/O-bound work is the cheaper of the two to have waiting.
        IoBudget = Math.Max(
            CpuBudget,
            settings.IoBudget ?? Math.Min(CpuBudget * settings.IoMultiplier, settings.MaxIoBudget));

        _cpu = new SemaphoreSlim(CpuBudget, CpuBudget);
        _io = new SemaphoreSlim(IoBudget, IoBudget);

        logger.LogInformation(
            "ETL concurrency: {CpuBudget} CPU / {IoBudget} I/O from {Cores} visible core(s), {Memory:F1} GiB available",
            CpuBudget, IoBudget, cores,
            GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / (1024.0 * 1024 * 1024));

        EnsureThreadPoolHeadroom(CpuBudget + settings.ReservedThreadPoolThreads, logger);
    }

    /// <summary>Units of CPU-bound work that may run at once.</summary>
    public int CpuBudget { get; }

    /// <summary>Units of I/O-bound work that may be in flight at once.</summary>
    public int IoBudget { get; }

    /// <summary>Maps <paramref name="source"/> concurrently within the workload's budget, returning
    /// the results in the order the inputs were given regardless of the order they completed in.
    ///
    /// Fails fast: the first failure cancels its siblings, and the exception that surfaces is the
    /// one worth reading - a stage's own diagnosis if there is one, never the cancellations that
    /// failure caused. The caller sees a single exception rather than an
    /// <see cref="AggregateException"/>, so a failed file reads the same as it did when the stage
    /// ran one file at a time.</summary>
    public async Task<IReadOnlyList<TResult>> ForEachAsync<TSource, TResult>(
        IReadOnlyList<TSource> source,
        EtlWorkload workload,
        Func<TSource, CancellationToken, Task<TResult>> body,
        CancellationToken cancellationToken)
    {
        if (source.Count == 0)
        {
            return [];
        }

        // Nothing to coordinate, and no permit worth taking to find that out.
        if (source.Count == 1)
        {
            return [await body(source[0], cancellationToken)];
        }

        var gate = GateFor(workload);
        var results = new TResult[source.Count];
        var tasks = new Task[source.Count];

        using var failFast = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        for (var index = 0; index < source.Count; index++)
        {
            tasks[index] = RunAsync(index);
        }

        try
        {
            await Task.WhenAll(tasks);
        }
        catch
        {
            // A caller that cancelled wants to hear that, not whatever the cancellation interrupted.
            cancellationToken.ThrowIfCancellationRequested();

            var failure = Diagnosable(tasks);

            if (failure is not null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }

            throw;
        }

        return results;

        async Task RunAsync(int index)
        {
            if (gate is not null)
            {
                await gate.WaitAsync(failFast.Token);
            }

            try
            {
                results[index] = await body(source[index], failFast.Token);
            }
            catch
            {
                // Not Cancel(): that would run every parked sibling's cancellation callback inline
                // on this thread, which is already busy failing.
                await failFast.CancelAsync();
                throw;
            }
            finally
            {
                gate?.Release();
            }
        }
    }

    public void Dispose()
    {
        _cpu.Dispose();
        _io.Dispose();
    }

    private SemaphoreSlim? GateFor(EtlWorkload workload) => workload switch
    {
        EtlWorkload.Cpu => _cpu,
        EtlWorkload.Io => _io,
        _ => null
    };

    /// <summary>The failure worth surfacing: a stage's own explanation first, then any real failure,
    /// and never a cancellation while something else went genuinely wrong. Null when the only thing
    /// that happened was cancellation, which the caller rethrows as it stands.</summary>
    private static Exception? Diagnosable(IEnumerable<Task> tasks)
    {
        var failures = tasks
            .Where(task => task.IsFaulted)
            .SelectMany(task => task.Exception!.InnerExceptions)
            .ToArray();

        return failures.FirstOrDefault(Explains)
            ?? failures.FirstOrDefault(failure => failure is not OperationCanceledException);
    }

    private static bool Explains(Exception exception)
    {
        for (Exception? cause = exception; cause is not null; cause = cause.InnerException)
        {
            if (cause is IEtlDiagnosableException)
            {
                return true;
            }
        }

        return false;
    }

    private static void EnsureThreadPoolHeadroom(int target, ILogger logger)
    {
        ThreadPool.GetMinThreads(out var workerThreads, out var completionPortThreads);

        if (workerThreads >= target)
        {
            return;
        }

        if (ThreadPool.SetMinThreads(target, completionPortThreads))
        {
            logger.LogInformation(
                "Raised minimum thread pool worker threads from {Previous} to {Target} so the health endpoint is not queued behind the ETL",
                workerThreads, target);
        }
        else
        {
            logger.LogWarning(
                "Could not raise minimum thread pool worker threads from {Previous} to {Target}; health probes may queue behind a fan-out",
                workerThreads, target);
        }
    }
}
