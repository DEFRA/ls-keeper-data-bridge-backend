using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Models;

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlDatasetListResponse
{
    public List<EtlDatasetResponse> Datasets { get; set; } = [];
}

[ExcludeFromCodeCoverage(Justification = "Response DTO - no logic to test.")]
public class EtlDatasetResponse
{
    /// <summary>The name to pass as the trigger's <c>dataset</c> filter.</summary>
    public required string Name { get; set; }

    /// <summary>Source filename prefix the discovery stage matches on, e.g. "LITP_SAMCPHHOLDING".</summary>
    public required string FilePrefixFormat { get; set; }

    /// <summary>How the source file is parsed: SimplePsv or Hcdt.</summary>
    public required string Format { get; set; }

    /// <summary>Snapshot or Delta.</summary>
    public required string IngestionMode { get; set; }
}
