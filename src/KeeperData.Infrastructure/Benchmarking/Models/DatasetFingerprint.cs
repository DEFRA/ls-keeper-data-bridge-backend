namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Fingerprint of a benchmark collection's data distribution.
/// </summary>
public sealed record DatasetFingerprint
{
    public string CollectionName { get; init; } = default!;
    public long DocumentCount { get; init; }
    public double AvgDocumentSizeBytes { get; init; }
    public double P95DocumentSizeBytes { get; init; }
}
