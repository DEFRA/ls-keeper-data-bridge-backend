using System.Runtime.CompilerServices;
using FluentAssertions;
using KeeperData.Core.Pipeline;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace KeeperData.Core.Tests.Unit.Pipeline;

// Fake stages defined here in the TEST project only - they prove the framework machinery without
// any domain code. They never ship.
public class PipelineFrameworkTests
{
    private static PipelineExecutor Executor() => new(NullLogger<PipelineExecutor>.Instance);
    private static IPipelineContext Context() => new TestContext();

    [Fact]
    public void Build_WithTwoStages_ShouldListStageNamesInOrder()
    {
        var definition = PipelineBuilder
            .InputSource(new IntSource(1))
            .Then(new DoubleStage())
            .Then(new SumStage())
            .Build();

        definition.GetStageNames().Should().Equal("double", "sum");
    }

    [Fact]
    public async Task Run_WithMapStage_ShouldTransformEachItemAndPipeToNext()
    {
        var captured = new List<int>();
        var definition = PipelineBuilder
            .InputSource(new IntSource(1, 2, 3))
            .Then(new DoubleStage())
            .Then(new CaptureStage<int>(captured))
            .Build();

        await Executor().RunAsync(definition, Context(), CancellationToken.None);

        captured.Should().Equal(2, 4, 6);
    }

    [Fact]
    public async Task Run_WithAggregateStage_ShouldCollapseStreamToSingleItem()
    {
        var captured = new List<int>();
        var definition = PipelineBuilder
            .InputSource(new IntSource(1, 2, 3))
            .Then(new SumStage())
            .Then(new CaptureStage<int>(captured))
            .Build();

        await Executor().RunAsync(definition, Context(), CancellationToken.None);

        captured.Should().Equal(6);
    }

    [Fact]
    public async Task Run_WithGroupStage_ShouldReduceManyToFewer()
    {
        var captured = new List<int>();
        var definition = PipelineBuilder
            .InputSource(new IntSource(1, 1, 2, 2, 3))
            .Then(new DistinctStage())
            .Then(new CaptureStage<int>(captured))
            .Build();

        await Executor().RunAsync(definition, Context(), CancellationToken.None);

        captured.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Run_WhenStageThrows_ShouldPropagateException()
    {
        var definition = PipelineBuilder
            .InputSource(new IntSource(1))
            .Then(new ThrowStage())
            .Build();

        Func<Task> act = () => Executor().RunAsync(definition, Context(), CancellationToken.None);

        await act.Should().ThrowAsync<PipelineExecutionException>();
    }

    [Fact]
    public async Task Run_WhenCancelled_ShouldThrowOperationCanceled()
    {
        var definition = PipelineBuilder
            .InputSource(new IntSource(1))
            .Then(new DelayStage())
            .Build();

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        Func<Task> act = () => Executor().RunAsync(definition, Context(), cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Run_WhenComplete_ShouldLogStageCompletion()
    {
        var loggerMock = new Mock<ILogger<PipelineExecutor>>();
        var executor = new PipelineExecutor(loggerMock.Object);
        var definition = PipelineBuilder
            .InputSource(new IntSource(1, 2, 3))
            .Then(new DoubleStage())
            .Build();

        await executor.RunAsync(definition, Context(), CancellationToken.None);

        loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("double")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
    }

    private sealed class TestContext : IPipelineContext { }

    private sealed class IntSource(params int[] items) : ISourceStage<int>
    {
        public string Name => "source";
        public async IAsyncEnumerable<int> RunAsync(IPipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var i in items) { cancellationToken.ThrowIfCancellationRequested(); yield return i; }
            await Task.CompletedTask;
        }
    }

    private sealed class DoubleStage : MapStage<int, int>
    {
        public override string Name => "double";
        protected override Task<int> MapAsync(int input, IPipelineContext context, CancellationToken cancellationToken) => Task.FromResult(input * 2);
    }

    private sealed class SumStage : AggregateStage<int, int>
    {
        public override string Name => "sum";
        protected override Task<int> AggregateAsync(IReadOnlyList<int> all, IPipelineContext context, CancellationToken cancellationToken) => Task.FromResult(all.Sum());
    }

    private sealed class DistinctStage : GroupStage<int, int>
    {
        public override string Name => "distinct";
        protected override async IAsyncEnumerable<int> GroupAsync(IAsyncEnumerable<int> input, IPipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var seen = new HashSet<int>();
            await foreach (var i in input.WithCancellation(cancellationToken))
            {
                if (seen.Add(i)) yield return i;
            }
        }
    }

    private sealed class CaptureStage<T>(List<T> sink) : MapStage<T, T>
    {
        public override string Name => "capture";
        protected override Task<T> MapAsync(T input, IPipelineContext context, CancellationToken cancellationToken)
        {
            sink.Add(input);
            return Task.FromResult(input);
        }
    }

    private sealed class ThrowStage : MapStage<int, int>
    {
        public override string Name => "throw";
        protected override Task<int> MapAsync(int input, IPipelineContext context, CancellationToken cancellationToken) => throw new InvalidOperationException("boom");
    }

    private sealed class DelayStage : MapStage<int, int>
    {
        public override string Name => "delay";
        protected override async Task<int> MapAsync(int input, IPipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(2000, cancellationToken);
            return input;
        }
    }
}
