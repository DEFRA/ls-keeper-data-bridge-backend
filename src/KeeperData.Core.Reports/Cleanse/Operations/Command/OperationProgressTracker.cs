using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;
using KeeperData.Core.Reports.Operations;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command;

/// <summary>
/// Holds the operation's mutable progress state in memory and periodically flushes
/// it to the database via a single background writer.  Phase callbacks mutate this
/// object directly (fast, in-memory), eliminating per-batch DB round-trips.
/// </summary>
public sealed class OperationProgressTracker
{
    private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(2);

    private readonly ICleanseAnalysisOperationAggRootRepository _repository;
    private readonly object _lock = new();

    private CleanseAnalysisOperation _operation = null!;
    private bool _isDirty;
    private volatile bool _cancellationRequested;

    public OperationProgressTracker(ICleanseAnalysisOperationAggRootRepository repository)
    {
        _repository = repository;
    }

    /// <summary>
    /// Whether cancellation has been requested for this operation.
    /// Refreshed from the database on each periodic flush tick.
    /// </summary>
    public bool IsCancellationRequested => _cancellationRequested;

    /// <summary>
    /// Loads the operation from the database into memory. Must be called once before
    /// any mutation or flush methods.
    /// </summary>
    public async Task InitializeAsync(string operationId, CancellationToken ct = default)
    {
        _operation = await _repository.GetByIdAsync(operationId, ct)
            ?? throw new InvalidOperationException($"Operation '{operationId}' not found.");
    }

    /// <summary>
    /// Replaces the unified operation tree progress snapshot (in-memory only).
    /// </summary>
    public void UpdateProgress(OperationNode progress)
    {
        lock (_lock)
        {
            _operation.Progress = progress;
            _isDirty = true;
        }
    }

    /// <summary>
    /// Runs the periodic flush loop that persists dirty state to the database every
    /// <see cref="FlushInterval"/> and refreshes the cancellation flag from the database.
    /// When <paramref name="operationTree"/> is provided, a unified progress tree snapshot
    /// is captured before each flush.
    /// Exits when <paramref name="ct"/> is cancelled.
    /// </summary>
    public async Task RunPeriodicFlushAsync(CancellationToken ct, OperationTree? operationTree = null)
    {
        using var timer = new PeriodicTimer(FlushInterval);
        while (await timer.WaitForNextTickAsync(ct))
        {
            if (operationTree is not null)
                UpdateProgress(operationTree.Snapshot());

            await FlushAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// Performs a single flush: persists dirty in-memory state to the database and
    /// refreshes the cancellation flag. Idempotent if no mutations have occurred.
    /// </summary>
    public async Task FlushAsync(CancellationToken ct)
    {
        // Snapshot progress fields under lock so the DB write happens on a consistent copy.
        ProgressSnapshot? snapshot;
        lock (_lock)
        {
            if (!_isDirty)
            {
                snapshot = null;
            }
            else
            {
                snapshot = CaptureSnapshot();
                _isDirty = false;
            }
        }

        if (snapshot is not null)
        {
            // Load the canonical document from DB to preserve fields set externally (e.g. CancellationRequested)
            var dbOperation = await _repository.GetByIdAsync(snapshot.Id, ct);
            if (dbOperation is not null)
            {
                ApplySnapshot(dbOperation, snapshot);
                await _repository.UpdateAsync(dbOperation, ct);
                _cancellationRequested = dbOperation.CancellationRequested;
                return;
            }
        }

        // Even when not dirty, refresh the cancellation flag from the database.
        var op = await _repository.GetByIdAsync(_operation.Id, ct);
        _cancellationRequested = op?.CancellationRequested ?? false;
    }

    #region Snapshot helpers

    /// <summary>Captures a deep-enough copy of the mutable progress fields. Must be called under <see cref="_lock"/>.</summary>
    private ProgressSnapshot CaptureSnapshot()
    {
        return new ProgressSnapshot(
            _operation.Id,
            _operation.Progress);
    }

    /// <summary>Applies snapshot values onto a DB-loaded aggregate root.</summary>
    private static void ApplySnapshot(CleanseAnalysisOperation target, ProgressSnapshot snapshot)
    {
        target.Progress = snapshot.Progress;
    }

    private sealed record ProgressSnapshot(
        string Id,
        OperationNode? Progress);

    #endregion
}
