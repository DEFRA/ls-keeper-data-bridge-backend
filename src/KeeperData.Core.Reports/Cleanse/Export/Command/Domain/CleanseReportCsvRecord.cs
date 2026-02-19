using CsvHelper.Configuration;
using KeeperData.Core.Reports.Issues.Query.Dtos;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Domain;


/// <summary>
/// CsvHelper mapping configuration for CleanseReportCsvRecord.
/// </summary>
public sealed class CleanseReportCsvMap : ClassMap<IssueDto>
{
    public CleanseReportCsvMap()
    {
        Map(m => m.Cph).Index(0).Name("CPH");
        Map(m => m.CtsLidFullIdentifier).Index(1).Name("CTS LID Full Identifier");
        Map(m => m.IssueCode).Index(2).Name("Issue Code");
        Map(m => m.RuleCode).Index(3).Name("Rule Code");
        Map(m => m.ErrorCode).Index(4).Name("Error Code");
        Map(m => m.ErrorDescription).Index(5).Name("Error Description");
        Map(m => m.EmailCTS).Index(6).Name("Email (CTS)").Convert(r => r.Value.EmailCTS is { Length: > 0 } e ? string.Join("; ", e) : string.Empty);
        Map(m => m.EmailSAM).Index(7).Name("Email (SAM)");
        Map(m => m.TelCTS).Index(8).Name("Tel (CTS)").Convert(r => r.Value.TelCTS is { Length: > 0 } t ? string.Join("; ", t) : string.Empty);
        Map(m => m.TelSAM).Index(9).Name("Tel (SAM)");
        Map(m => m.FSA).Index(10).Name("FSA");
        Map(m => m.CreatedAtUtc).Index(11).Name("First Detected (UTC)");
        Map(m => m.LastUpdatedAtUtc).Index(12).Name("Last Updated (UTC)");
        Map(m => m.IsActive).Index(13).Name("Active");
        Map(m => m.IsIgnored).Index(14).Name("Ignored");
        Map(m => m.ResolutionStatus).Index(15).Name("Resolution Status");
        Map(m => m.AssignedTo).Index(16).Name("Assigned To");
    }
}
