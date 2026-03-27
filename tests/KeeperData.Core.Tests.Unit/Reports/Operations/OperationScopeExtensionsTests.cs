using FluentAssertions;
using KeeperData.Core.Reports.Operations;
using Microsoft.Extensions.Time.Testing;

namespace KeeperData.Core.Tests.Unit.Reports.Operations;

public class OperationScopeExtensionsTests
{
    private readonly FakeTimeProvider _timeProvider = new(new DateTimeOffset(2025, 1, 15, 10, 0, 0, TimeSpan.Zero));

    private OperationScope CreateScope(string name = "test")
    {
        var tree = new OperationTree(_timeProvider);
        return tree.CreateScope(name);
    }

    #region RunAsync (void)

    [Fact]
    public async Task RunAsync_Success_ShouldCompleteScope()
    {
        var scope = CreateScope();
        scope.Start();

        await scope.RunAsync(() => Task.CompletedTask);

        var snapshot = scope.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Completed);
    }

    [Fact]
    public async Task RunAsync_OperationCancelled_ShouldCancelScopeAndRethrow()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync(() => throw new OperationCanceledException());

        await act.Should().ThrowAsync<OperationCanceledException>();
        scope.Snapshot().Status.Should().Be(OperationStatuses.Cancelled);
    }

    [Fact]
    public async Task RunAsync_OperationCancelled_ShouldUseDefaultCancelDescription()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync(() => throw new OperationCanceledException());

        await act.Should().ThrowAsync<OperationCanceledException>();
        scope.Snapshot().Description.Should().Be("Cancelled");
    }

    [Fact]
    public async Task RunAsync_OperationCancelled_ShouldUseCustomCancelDescription()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync(
            () => throw new OperationCanceledException(),
            cancelDescription: "User cancelled");

        await act.Should().ThrowAsync<OperationCanceledException>();
        scope.Snapshot().Description.Should().Be("User cancelled");
    }

    [Fact]
    public async Task RunAsync_Exception_ShouldFailScopeAndRethrow()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync(() => throw new InvalidOperationException("boom"));

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("boom");
        scope.Snapshot().Status.Should().Be(OperationStatuses.Failed);
    }

    [Fact]
    public async Task RunAsync_Exception_ShouldUseExceptionMessageAsFailDescription()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync(() => throw new InvalidOperationException("something broke"));

        await act.Should().ThrowAsync<InvalidOperationException>();
        scope.Snapshot().Description.Should().Be("something broke");
    }

    [Fact]
    public async Task RunAsync_Exception_ShouldUseCustomFailDescription()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync(
            () => throw new InvalidOperationException("boom"),
            failDescription: "Phase failed");

        await act.Should().ThrowAsync<InvalidOperationException>();
        scope.Snapshot().Description.Should().Be("Phase failed");
    }

    [Fact]
    public async Task RunAsync_NullScope_Success_ShouldNotThrow()
    {
        OperationScope? scope = null;

        await scope.RunAsync(() => Task.CompletedTask);
    }

    [Fact]
    public async Task RunAsync_NullScope_Exception_ShouldRethrow()
    {
        OperationScope? scope = null;

        var act = () => scope.RunAsync(() => throw new InvalidOperationException("boom"));

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task RunAsync_NullScope_OperationCancelled_ShouldRethrow()
    {
        OperationScope? scope = null;

        var act = () => scope.RunAsync(() => throw new OperationCanceledException());

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    #endregion

    #region RunAsync<T> (with return value)

    [Fact]
    public async Task RunAsyncT_Success_ShouldReturnResultAndCompleteScope()
    {
        var scope = CreateScope();
        scope.Start();

        var result = await scope.RunAsync(() => Task.FromResult(42));

        result.Should().Be(42);
        scope.Snapshot().Status.Should().Be(OperationStatuses.Completed);
    }

    [Fact]
    public async Task RunAsyncT_OperationCancelled_ShouldCancelScopeAndRethrow()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync<int>(() => throw new OperationCanceledException());

        await act.Should().ThrowAsync<OperationCanceledException>();
        scope.Snapshot().Status.Should().Be(OperationStatuses.Cancelled);
    }

    [Fact]
    public async Task RunAsyncT_OperationCancelled_ShouldUseCustomCancelDescription()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync<int>(
            () => throw new OperationCanceledException(),
            cancelDescription: "Pump cancelled");

        await act.Should().ThrowAsync<OperationCanceledException>();
        scope.Snapshot().Description.Should().Be("Pump cancelled");
    }

    [Fact]
    public async Task RunAsyncT_Exception_ShouldFailScopeAndRethrow()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync<int>(() => throw new InvalidOperationException("fail"));

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("fail");
        scope.Snapshot().Status.Should().Be(OperationStatuses.Failed);
    }

    [Fact]
    public async Task RunAsyncT_Exception_ShouldUseCustomFailDescription()
    {
        var scope = CreateScope();
        scope.Start();

        var act = () => scope.RunAsync<int>(
            () => throw new InvalidOperationException("fail"),
            failDescription: "Export failed");

        await act.Should().ThrowAsync<InvalidOperationException>();
        scope.Snapshot().Description.Should().Be("Export failed");
    }

    [Fact]
    public async Task RunAsyncT_NullScope_Success_ShouldReturnResult()
    {
        OperationScope? scope = null;

        var result = await scope.RunAsync(() => Task.FromResult(99));

        result.Should().Be(99);
    }

    [Fact]
    public async Task RunAsyncT_NullScope_Exception_ShouldRethrow()
    {
        OperationScope? scope = null;

        var act = () => scope.RunAsync<int>(() => throw new InvalidOperationException("boom"));

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    #endregion
}
