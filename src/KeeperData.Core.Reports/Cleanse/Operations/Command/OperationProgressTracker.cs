using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;

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
    /// Updates the operation-level progress counters (in-memory only).
    /// </summary>
    public void UpdateProgress(
        double progressPercentage,
        string statusDescription,
        int recordsAnalyzed,
        int totalRecords,
        int issuesFound,
        int issuesResolved)
    {
        lock (_lock)
        {
            _operation.UpdateProgress(progressPercentage, statusDescription,
                recordsAnalyzed, totalRecords, issuesFound, issuesResolved);
            _isDirty = true;
        }
    }

    /// <summary>
    /// Marks a phase as running (in-memory only).
    /// </summary>
    public void StartPhase(OperationPhase phase, int totalRecords)
    {
        lock (_lock)
        {
            _operation.StartPhase(phase, totalRecords);
            _isDirty = true;
        }
    }

    /// <summary>
    /// Updates the progress counters for a specific phase (in-memory only).
    /// </summary>
    public void UpdatePhaseProgress(OperationPhase phase, int recordsProcessed, int totalRecords, string description)
    {
        lock (_lock)
        {
            _operation.UpdatePhaseProgress(phase, recordsProcessed, totalRecords, description);
            _isDirty = true;
        }
    }

    /// <summary>
    /// Marks a phase as completed (in-memory only).
    /// </summary>
    public void CompletePhase(OperationPhase phase)
    {
        lock (_lock)
        {
            _operation.CompletePhase(phase);
            _isDirty = true;
        }
    }

    /// <summary>
    /// Replaces the timing tree snapshot (in-memory only).
    /// </summary>
    public void UpdateTimings(TimingNode timings)
    {
        lock (_lock)
        {
            _operation.UpdateTimings(timings);
            _isDirty = true;
        }
    }

    /// <summary>
    /// Runs the periodic flush loop that persists dirty state to the database every
    /// <see cref="FlushInterval"/> and refreshes the cancellation flag from the database.
    /// When <paramref name="timings"/> is provided, a timing tree snapshot is captured
    /// into the in-memory operation before each flush.
    /// Exits when <paramref name="ct"/> is cancelled.
    /// </summary>
    public async Task RunPeriodicFlushAsync(TimingTree? timings, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(FlushInterval);
        while (await timer.WaitForNextTickAsync(ct))
        {
            if (timings is not null)
                UpdateTimings(timings.Snapshot("Analysis"));

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
        var phasesCopy = _operation.Phases.Select(p => new PhaseSnapshot(
            p.Name, p.Status, p.Percentage, p.Description,
            p.RecordsProcessed, p.TotalRecords,
            p.StartedAtUtc, p.CompletedAtUtc, p.DurationMs)).ToList();

        return new ProgressSnapshot(
            _operation.Id,
            _operation.ProgressPercentage,
            _operation.StatusDescription,
            _operation.RecordsAnalyzed,
            _operation.TotalRecords,
            _operation.IssuesFound,
            _operation.IssuesResolved,
            _operation.CurrentPhase,
            phasesCopy,
            _operation.Timings);
    }

    /// <summary>Applies snapshot values onto a DB-loaded aggregate root.</summary>
    private static void ApplySnapshot(CleanseAnalysisOperation target, ProgressSnapshot snapshot)
    {
        target.ProgressPercentage = snapshot.ProgressPercentage;
        target.StatusDescription = snapshot.StatusDescription;
        target.RecordsAnalyzed = snapshot.RecordsAnalyzed;
        target.TotalRecords = snapshot.TotalRecords;
        target.IssuesFound = snapshot.IssuesFound;
        target.IssuesResolved = snapshot.IssuesResolved;
        target.CurrentPhase = snapshot.CurrentPhase;
        target.Timings = snapshot.Timings;

        foreach (var ps in snapshot.Phases)
        {
            var targetPhase = target.Phases.Find(p => p.Name == ps.Name);
            if (targetPhase is null) continue;

            targetPhase.Status = ps.Status;
            targetPhase.Percentage = ps.Percentage;
            targetPhase.Description = ps.Description;
            targetPhase.RecordsProcessed = ps.RecordsProcessed;
            targetPhase.TotalRecords = ps.TotalRecords;
            targetPhase.StartedAtUtc = ps.StartedAtUtc;
            targetPhase.CompletedAtUtc = ps.CompletedAtUtc;
            targetPhase.DurationMs = ps.DurationMs;
        }
    }

    private sealed record ProgressSnapshot(
        string Id,
        double ProgressPercentage,
        string StatusDescription,
        int RecordsAnalyzed,
        int TotalRecords,
        int IssuesFound,
        int IssuesResolved,
        string? CurrentPhase,
        List<PhaseSnapshot> Phases,
        TimingNode? Timings);

    private sealed record PhaseSnapshot(
        string Name,
        string Status,
        double Percentage,
        string Description,
        int RecordsProcessed,
        int TotalRecords,
        DateTime? StartedAtUtc,
        DateTime? CompletedAtUtc,
        long? DurationMs);

    #endregion
}
