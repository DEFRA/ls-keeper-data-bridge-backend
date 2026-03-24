using KeeperData.Infrastructure.Benchmarking.Models;
using MongoDB.Driver.Core.Events;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace KeeperData.Infrastructure.Benchmarking.Metrics;

/// <summary>
/// Subscribes to MongoDB driver events and aggregates command latency,
/// connection pool checkout wait times, failures, and pool lifecycle events.
/// Thread-safe; designed to be wired via <see cref="MongoDB.Driver.MongoClientSettings.ClusterConfigurator"/>.
/// </summary>
public sealed class DriverEventCollector
{
    private readonly ConcurrentDictionary<int, long> _commandTimestamps = new();
    private readonly ConcurrentDictionary<string, LatencyRecorder> _commandLatencies = new();
    private readonly ConcurrentDictionary<string, int> _commandFailures = new();

    private readonly ConcurrentDictionary<long, long> _checkoutTimestamps = new();
    private readonly LatencyRecorder _checkoutWait = new();

    private int _checkoutFailures;
    private int _connectionsCreated;
    private int _connectionsClosed;
    private int _poolCleared;

    // ── Command events ────────────────────────────────────────────────

    public void OnCommandStarted(CommandStartedEvent e)
    {
        _commandTimestamps[e.RequestId] = Stopwatch.GetTimestamp();
    }

    public void OnCommandSucceeded(CommandSucceededEvent e)
    {
        if (_commandTimestamps.TryRemove(e.RequestId, out var start))
        {
            var elapsed = Stopwatch.GetElapsedTime(start);
            var recorder = _commandLatencies.GetOrAdd(e.CommandName, _ => new LatencyRecorder());
            recorder.Record(elapsed);
        }
    }

    public void OnCommandFailed(CommandFailedEvent e)
    {
        _commandTimestamps.TryRemove(e.RequestId, out _);
        _commandFailures.AddOrUpdate(e.CommandName, 1, (_, v) => v + 1);
    }

    // ── Connection pool events ────────────────────────────────────────

    public void OnCheckingOut(ConnectionPoolCheckingOutConnectionEvent e)
    {
        if (e.OperationId.HasValue)
            _checkoutTimestamps[e.OperationId.Value] = Stopwatch.GetTimestamp();
    }

    public void OnCheckedOut(ConnectionPoolCheckedOutConnectionEvent e)
    {
        if (e.OperationId.HasValue && _checkoutTimestamps.TryRemove(e.OperationId.Value, out var start))
        {
            _checkoutWait.Record(Stopwatch.GetElapsedTime(start));
        }
    }

    public void OnCheckoutFailed(ConnectionPoolCheckingOutConnectionFailedEvent e)
    {
        if (e.OperationId.HasValue)
            _checkoutTimestamps.TryRemove(e.OperationId.Value, out _);
        Interlocked.Increment(ref _checkoutFailures);
    }

    public void OnConnectionCreated(ConnectionCreatedEvent _) =>
        Interlocked.Increment(ref _connectionsCreated);

    public void OnConnectionClosed(ConnectionClosedEvent _) =>
        Interlocked.Increment(ref _connectionsClosed);

    public void OnPoolCleared(ConnectionPoolClearedEvent _) =>
        Interlocked.Increment(ref _poolCleared);

    // ── Snapshot ──────────────────────────────────────────────────────

    public DriverMetrics ToMetrics() => new()
    {
        CommandLatency = _commandLatencies.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.Compute()),
        CommandFailures = _commandFailures.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value),
        ConnectionCheckoutWait = _checkoutWait.Count > 0 ? _checkoutWait.Compute() : null,
        CheckoutFailures = _checkoutFailures,
        ConnectionsCreated = _connectionsCreated,
        ConnectionsClosed = _connectionsClosed,
        PoolClearedEvents = _poolCleared
    };
}
