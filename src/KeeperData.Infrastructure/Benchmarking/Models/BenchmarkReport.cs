namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// The full benchmark report emitted at the end of a run.
/// Designed to be JSON-serialised and compared across environments.
/// </summary>
public sealed record BenchmarkReport
{
    public string Environment { get; init; } = System.Environment.MachineName;
    public DateTime TimestampUtc { get; init; } = DateTime.UtcNow;
    public BenchmarkConfig Config { get; init; } = default!;
    public string Status { get; init; } = default!;
    public double TotalElapsedSeconds { get; init; }

    public IReadOnlyList<DatasetFingerprint> DatasetFingerprints { get; init; }
        = Array.Empty<DatasetFingerprint>();

    public IReadOnlyList<IndexFingerprint> IndexFingerprints { get; init; }
        = Array.Empty<IndexFingerprint>();

    public IReadOnlyList<ScenarioResult> ScenarioResults { get; init; }
        = Array.Empty<ScenarioResult>();

    public DriverMetrics DriverMetrics { get; init; } = default!;

    public IReadOnlyList<ExplainResult> ExplainResults { get; init; }
        = Array.Empty<ExplainResult>();

    /// <summary>
    /// Noisy-neighbour diagnostic analysis. Populated after the run completes.
    /// When comparing environments, check <see cref="NoisyNeighbourAnalysis.HasRedFlags"/>
    /// — flags present in production but absent locally confirm resource contention.
    /// </summary>
    public NoisyNeighbourAnalysis? NoisyNeighbourAnalysis { get; init; }
}
