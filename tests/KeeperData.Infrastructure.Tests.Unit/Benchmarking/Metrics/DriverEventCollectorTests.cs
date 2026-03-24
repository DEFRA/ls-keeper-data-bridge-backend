using FluentAssertions;
using KeeperData.Infrastructure.Benchmarking.Metrics;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Servers;
using System.Net;

namespace KeeperData.Infrastructure.Tests.Unit.Benchmarking.Metrics;

public class DriverEventCollectorTests
{
    private static readonly ServerId s_serverId = new(new ClusterId(), new DnsEndPoint("localhost", 27017));
    private static readonly ConnectionId s_connectionId = new(s_serverId);
    private static readonly BsonDocument s_emptyDoc = new();
    private static readonly DatabaseNamespace s_dbNs = new("test");

    private static CommandStartedEvent StartedEvent(string cmd, int requestId) =>
        new(cmd, s_emptyDoc, s_dbNs, null, requestId, s_connectionId);

    private static CommandSucceededEvent SucceededEvent(string cmd, int requestId) =>
        new(cmd, s_emptyDoc, s_dbNs, null, requestId, s_connectionId, TimeSpan.Zero);

    private static CommandFailedEvent FailedEvent(string cmd, int requestId) =>
        new(cmd, s_dbNs, new Exception("test"), null, requestId, s_connectionId, TimeSpan.Zero);

    [Fact]
    public void ToMetrics_EmptyCollector_ReturnsEmptyMetrics()
    {
        var collector = new DriverEventCollector();

        var metrics = collector.ToMetrics();

        metrics.CommandLatency.Should().BeEmpty();
        metrics.CommandFailures.Should().BeEmpty();
        metrics.ConnectionCheckoutWait.Should().BeNull();
        metrics.CheckoutFailures.Should().Be(0);
        metrics.ConnectionsCreated.Should().Be(0);
        metrics.ConnectionsClosed.Should().Be(0);
        metrics.PoolClearedEvents.Should().Be(0);
    }

    [Fact]
    public void CommandStartedAndSucceeded_RecordsLatency()
    {
        var collector = new DriverEventCollector();

        collector.OnCommandStarted(StartedEvent("find", 1));
        Thread.Sleep(5);
        collector.OnCommandSucceeded(SucceededEvent("find", 1));

        var metrics = collector.ToMetrics();

        metrics.CommandLatency.Should().ContainKey("find");
        metrics.CommandLatency["find"].AvgMs.Should().BeGreaterThan(0);
    }

    [Fact]
    public void CommandFailed_RecordsFailureCount()
    {
        var collector = new DriverEventCollector();

        collector.OnCommandStarted(StartedEvent("insert", 1));
        collector.OnCommandFailed(FailedEvent("insert", 1));

        collector.OnCommandStarted(StartedEvent("insert", 2));
        collector.OnCommandFailed(FailedEvent("insert", 2));

        var metrics = collector.ToMetrics();

        metrics.CommandFailures.Should().ContainKey("insert");
        metrics.CommandFailures["insert"].Should().Be(2);
    }

    [Fact]
    public void CommandSucceeded_WithoutStart_DoesNotRecordLatency()
    {
        var collector = new DriverEventCollector();

        collector.OnCommandSucceeded(SucceededEvent("find", 999));

        var metrics = collector.ToMetrics();
        metrics.CommandLatency.Should().BeEmpty();
    }

    [Fact]
    public void MultipleCommandTypes_RecordsSeparateLatencies()
    {
        var collector = new DriverEventCollector();

        collector.OnCommandStarted(StartedEvent("find", 1));
        collector.OnCommandSucceeded(SucceededEvent("find", 1));

        collector.OnCommandStarted(StartedEvent("insert", 2));
        collector.OnCommandSucceeded(SucceededEvent("insert", 2));

        var metrics = collector.ToMetrics();

        metrics.CommandLatency.Should().ContainKey("find");
        metrics.CommandLatency.Should().ContainKey("insert");
    }

    [Fact]
    public void ConnectionCreated_IncrementsCounter()
    {
        var collector = new DriverEventCollector();

        collector.OnConnectionCreated(new ConnectionCreatedEvent(s_connectionId, default, 0L));
        collector.OnConnectionCreated(new ConnectionCreatedEvent(s_connectionId, default, 0L));

        var metrics = collector.ToMetrics();
        metrics.ConnectionsCreated.Should().Be(2);
    }

    [Fact]
    public void ConnectionClosed_IncrementsCounter()
    {
        var collector = new DriverEventCollector();

        collector.OnConnectionClosed(new ConnectionClosedEvent(s_connectionId, default, 0L));

        var metrics = collector.ToMetrics();
        metrics.ConnectionsClosed.Should().Be(1);
    }

    [Fact]
    public void PoolCleared_IncrementsCounter()
    {
        var collector = new DriverEventCollector();

        collector.OnPoolCleared(new ConnectionPoolClearedEvent(s_serverId, default, false));
        collector.OnPoolCleared(new ConnectionPoolClearedEvent(s_serverId, default, false));
        collector.OnPoolCleared(new ConnectionPoolClearedEvent(s_serverId, default, false));

        var metrics = collector.ToMetrics();
        metrics.PoolClearedEvents.Should().Be(3);
    }

    [Fact]
    public void CheckingOutAndCheckedOut_RecordsCheckoutWait()
    {
        var collector = new DriverEventCollector();

        collector.OnCheckingOut(new ConnectionPoolCheckingOutConnectionEvent(s_serverId, 1L));
        Thread.Sleep(5);
        collector.OnCheckedOut(new ConnectionPoolCheckedOutConnectionEvent(s_connectionId, TimeSpan.Zero, 1L));

        var metrics = collector.ToMetrics();
        metrics.ConnectionCheckoutWait.Should().NotBeNull();
        metrics.ConnectionCheckoutWait!.AvgMs.Should().BeGreaterThan(0);
    }

    [Fact]
    public void CheckoutFailed_IncrementsCounter()
    {
        var collector = new DriverEventCollector();

        collector.OnCheckingOut(new ConnectionPoolCheckingOutConnectionEvent(s_serverId, 1L));
        collector.OnCheckoutFailed(new ConnectionPoolCheckingOutConnectionFailedEvent(
            s_serverId, new Exception("pool exhausted"), 1L, TimeSpan.Zero,
            ConnectionCheckOutFailedReason.Timeout));

        var metrics = collector.ToMetrics();
        metrics.CheckoutFailures.Should().Be(1);
    }

    [Fact]
    public void CheckingOut_NullOperationId_DoesNotTrack()
    {
        var collector = new DriverEventCollector();

        collector.OnCheckingOut(new ConnectionPoolCheckingOutConnectionEvent(s_serverId, null));
        collector.OnCheckedOut(new ConnectionPoolCheckedOutConnectionEvent(s_connectionId, TimeSpan.Zero, null));

        var metrics = collector.ToMetrics();
        metrics.ConnectionCheckoutWait.Should().BeNull();
    }
}
