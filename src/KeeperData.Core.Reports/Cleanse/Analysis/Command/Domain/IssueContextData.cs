namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

/// <summary>
/// Optional issue data to persist on the report item.
/// A nullable subset of <see cref="Domain.CleanseReportItem"/> optional fields.
/// </summary>
public sealed class IssueContextData
{
    public string[]? EmailCTS { get; set; }
    public string? EmailSAM { get; set; }
    public string[]? TelCTS { get; set; }
    public string? TelSAM { get; set; }
    public string? FSA { get; set; }
    public string? CtsLidFullIdentifier { get; set; }
    public string? SamCph { get; set; }
    public string? AnimalSpeciesCode { get; set; }
    public string? LocationNameSAM { get; set; }
    public string? LocationNameCTS { get; set; }
}
