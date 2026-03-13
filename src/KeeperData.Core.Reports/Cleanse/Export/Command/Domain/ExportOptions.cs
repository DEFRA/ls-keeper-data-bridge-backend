using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Domain;

/// <summary>
/// Options controlling the behaviour of a cleanse report export.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record ExportOptions
{
    /// <summary>
    /// When set, only issues updated at or after this UTC timestamp are exported.
    /// When null, all active issues are exported (full extract).
    /// </summary>
    public DateTime? Since { get; init; }

    /// <summary>
    /// Whether to send an email notification after a successful export.
    /// </summary>
    public bool SendNotification { get; init; } = true;
}
