using CsvHelper.Configuration;
using KeeperData.Core.Reports.Issues.Query.Dtos;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Domain;


/// <summary>
/// CsvHelper mapping configuration for CleanseReportCsvRecord.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "CSV mapping configuration - covered by integration tests.")]
public sealed class CleanseReportCsvMap : ClassMap<IssueDto>
{
    public CleanseReportCsvMap()
    {
        Map(m => m.Cph).Index(0).Name("CPH");
        Map(m => m.RuleCode).Index(1).Name("Rule No");
        Map(m => m.ErrorCode).Index(2).Name("Error Code");
        Map(m => m.ErrorDescription).Index(3).Name("Error Description");
        Map(m => m.EmailCTS).Index(4).Name("Email CTS").Convert(r => r.Value.EmailCTS is { Length: > 0 } e ? string.Join(";", e) : string.Empty);
        Map(m => m.EmailSAM).Index(5).Name("Email SAM");
        Map(m => m.TelCTS).Index(6).Name("Tel CTS").Convert(r => r.Value.TelCTS is { Length: > 0 } t ? string.Join(";", t) : string.Empty);
        Map(m => m.TelSAM).Index(7).Name("Tel SAM");
        Map(m => m.FSA).Index(8).Name("FSA");
    }
}
