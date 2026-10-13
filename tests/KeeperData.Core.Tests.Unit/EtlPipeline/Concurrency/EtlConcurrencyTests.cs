using FluentAssertions;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Concurrency;
using KeeperData.Core.EtlPipeline.Setup;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Concurrency;

/// <summary>The fan-out every stage runs on. What matters is that a payload does not depend on
/// which file finished first, that the budget is actually a limit rather than a hint, and that a
/// failure still reads the way it did when the stages ran one file at a time.</summary>
[Trait("Category", "Unit")]
public class EtlConcurrencyTests
{
    [Fact]
    public async Task Returns_results_in_input_order_whatever_order_they_finish_in()
    {
        var concurrency = TestConcurrency.Of(new EtlConcurrencyOptions { CpuBudget = 8 });
        int[] input = [0, 1, 2, 3, 4, 5, 6, 7];

        // Later items finish first, so completion order is the reverse of input order.
        var results = await concurrency.ForEachAsync(
            input,
            EtlWorkload.Cpu,
            async (item, token) =>
            {
                await Task.Delay((input.Length - item) * 10, token);
                return item;
            },
            CancellationToken.None);

        results.Should().Equal(input);
    }

    [Theory]
    [InlineData(EtlWorkload.Cpu)]
    [InlineData(EtlWorkload.Io)]
    public async Task Never_runs_more_than_the_budget_at_once(EtlWorkload workload)
    {
        const int budget = 3;

        var concurrency = TestConcurrency.Of(
            new EtlConcurrencyOptions { CpuBudget = budget, IoBudget = budget });

        var running = 0;
        var peak = 0;

        await concurrency.ForEachAsync(
            Enumerable.Range(0, 40).ToArray(),
            workload,
            async (item, token) =>
            {
                var current = Interlocked.Increment(ref running);
                InterlockedMax(ref peak, current);

                await Task.Delay(5, token);

                Interlocked.Decrement(ref running);
                return item;
            },
            CancellationToken.None);

        peak.Should().BeLessThanOrEqualTo(budget);
        peak.Should().BeGreaterThan(1, "the work should actually have overlapped");
    }

    /// <summary>The nesting the stages use: a stage fans out over datasets against no budget, and
    /// each dataset fans out over its files against one. An outer task that took a permit and then
    /// waited for an inner one could hold the whole budget and wait on itself.</summary>
    [Fact]
    public async Task Unbounded_work_nested_over_budgeted_work_does_not_deadlock()
    {
        const int budget = 2;
        var concurrency = TestConcurrency.Of(new EtlConcurrencyOptions { CpuBudget = budget });

        var datasets = Enumerable.Range(0, budget * 8).ToArray();

        var totals = await concurrency.ForEachAsync(
            datasets,
            EtlWorkload.Unbounded,
            async (dataset, token) =>
            {
                var files = await concurrency.ForEachAsync(
                    Enumerable.Range(0, 4).ToArray(),
                    EtlWorkload.Cpu,
                    async (file, inner) =>
                    {
                        await Task.Delay(1, inner);
                        return file;
                    },
                    token);

                return dataset + files.Count;
            },
            CancellationToken.None);

        totals.Should().Equal(datasets.Select(dataset => dataset + 4));
    }

    [Fact]
    public async Task Cancels_its_siblings_once_one_item_fails()
    {
        const int budget = 4;
        var concurrency = TestConcurrency.Of(new EtlConcurrencyOptions { CpuBudget = budget });

        var siblingsRunning = new TaskCompletionSource();
        var running = 0;
        var cancelled = 0;

        var act = () => concurrency.ForEachAsync(
            Enumerable.Range(0, 64).ToArray(),
            EtlWorkload.Cpu,
            async (item, token) =>
            {
                // The first item holds a permit and waits, so the rest of the budget fills with
                // siblings that are genuinely in flight when it fails.
                if (item == 0)
                {
                    await siblingsRunning.Task;
                    throw new InvalidOperationException("first file failed");
                }

                if (Interlocked.Increment(ref running) == budget - 1)
                {
                    siblingsRunning.SetResult();
                }

                try
                {
                    await Task.Delay(Timeout.Infinite, token);
                }
                catch (OperationCanceledException)
                {
                    Interlocked.Increment(ref cancelled);
                    throw;
                }

                return item;
            },
            CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("first file failed");

        cancelled.Should().BeGreaterThanOrEqualTo(budget - 1,
            "every sibling that was already running should have been told to stop");
    }

    /// <summary>A stage's own explanation is what import status serves, so it has to survive the
    /// fan-out intact rather than arriving wrapped in an AggregateException behind an opaque
    /// failure that happened to be raised first.</summary>
    [Fact]
    public async Task Surfaces_the_stages_own_diagnosis_rather_than_an_opaque_failure()
    {
        var concurrency = TestConcurrency.Of(new EtlConcurrencyOptions { CpuBudget = 8 });

        var opaqueReady = new TaskCompletionSource();
        var diagnosedReady = new TaskCompletionSource();

        var act = () => concurrency.ForEachAsync(
            Enumerable.Range(0, 4).ToArray(),
            EtlWorkload.Cpu,
            async (item, token) =>
            {
                // Both failures have to land, so neither throws until the other is in position.
                // The opaque one is the earlier item, so position cannot be what decides this.
                if (item == 0)
                {
                    opaqueReady.SetResult();
                    await diagnosedReady.Task;
                    throw new IOException("a technically accurate and useless message");
                }

                if (item == 1)
                {
                    diagnosedReady.SetResult();
                    await opaqueReady.Task;
                    throw new DiagnosableFailure("file 'x' for dataset 'y' could not be decrypted");
                }

                await Task.Delay(Timeout.Infinite, token);
                return item;
            },
            CancellationToken.None);

        var thrown = await act.Should().ThrowAsync<DiagnosableFailure>();
        thrown.Which.Message.Should().Be("file 'x' for dataset 'y' could not be decrypted");
    }

    [Fact]
    public async Task Reports_the_callers_cancellation_as_cancellation()
    {
        var concurrency = TestConcurrency.Of(new EtlConcurrencyOptions { CpuBudget = 4 });

        using var cancellation = new CancellationTokenSource();

        var act = () => concurrency.ForEachAsync(
            Enumerable.Range(0, 16).ToArray(),
            EtlWorkload.Cpu,
            async (item, token) =>
            {
                await cancellation.CancelAsync();
                await Task.Delay(Timeout.Infinite, token);
                return item;
            },
            cancellation.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Maps_nothing_when_given_nothing()
    {
        var results = await TestConcurrency.Default.ForEachAsync(
            Array.Empty<int>(),
            EtlWorkload.Cpu,
            (item, _) => Task.FromResult(item),
            CancellationToken.None);

        results.Should().BeEmpty();
    }

    [Fact]
    public void Leaves_a_core_for_the_host_and_lets_io_run_further_ahead()
    {
        using var concurrency = TestConcurrency.Of(new EtlConcurrencyOptions { ReservedCores = 1 });

        concurrency.CpuBudget.Should().Be(Math.Max(1, Environment.ProcessorCount - 1));
        concurrency.IoBudget.Should().BeGreaterThanOrEqualTo(concurrency.CpuBudget);
    }

    [Fact]
    public void Keeps_a_core_even_where_reserving_one_would_leave_none()
    {
        using var concurrency = TestConcurrency.Of(
            new EtlConcurrencyOptions { ReservedCores = Environment.ProcessorCount + 4 });

        concurrency.CpuBudget.Should().Be(1);
        concurrency.IoBudget.Should().BeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public void Caps_the_derived_io_budget()
    {
        using var concurrency = TestConcurrency.Of(
            new EtlConcurrencyOptions { CpuBudget = 64, IoMultiplier = 4, MaxIoBudget = 32 });

        // The cap is below the CPU budget here, and I/O is never the narrower of the two.
        concurrency.IoBudget.Should().Be(64);
    }

    /// <summary>The budget only limits anything if every stage charges against the same one.</summary>
    [Fact]
    public void Is_registered_once_for_the_whole_host()
    {
        // Logging comes from the host, as it does for every other stage the pipeline registers.
        using var provider = new ServiceCollection()
            .AddSingleton(typeof(ILogger<>), typeof(NullLogger<>))
            .AddEtlPipeline()
            .BuildServiceProvider();

        var concurrency = provider.GetRequiredService<EtlConcurrency>();

        concurrency.CpuBudget.Should().BeGreaterThan(0);
        provider.GetRequiredService<EtlConcurrency>().Should().BeSameAs(concurrency);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref target)))
        {
            if (Interlocked.CompareExchange(ref target, value, current) == current)
            {
                return;
            }
        }
    }

    private sealed class DiagnosableFailure(string message) : Exception(message), IEtlDiagnosableException;
}
