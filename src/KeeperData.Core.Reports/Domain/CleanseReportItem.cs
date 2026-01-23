namespace KeeperData.Core.Reports.Domain;

/// <summary>
/// Represents a data quality issue detected during cleanse analysis.
/// </summary>
public class CleanseReportItem
{
    /// <summary>
    /// Gets or sets the unique identifier (thumbprint/hash of the identifier).
    /// </summary>
    public required string Id { get; set; }

    /// <summary>
    /// Gets or sets the issue code (e.g., 'CTS_CPH_NOT_IN_SAM').
    /// </summary>
    public required string Code { get; set; }

    /// <summary>
    /// Gets or sets the CTS LID full identifier value.
    /// </summary>
    public required string CtsLidFullIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the CPH value.
    /// </summary>
    public required string Cph { get; set; }

    /// <summary>
    /// Gets or sets the timestamp when the issue was first detected.
    /// </summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>
    /// Gets or sets the timestamp when the issue was last checked.
    /// </summary>
    public DateTime LastUpdatedAtUtc { get; set; }

    /// <summary>
    /// Gets or sets whether the issue is currently active.
    /// </summary>
    public bool IsActive { get; set; }
}
