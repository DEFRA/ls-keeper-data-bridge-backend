using CsvHelper.Configuration;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Impl;

/// <summary>
/// CSV record for cleanse report export.
/// </summary>
public sealed class CleanseReportCsvRecord
{
    public required string CPH { get; init; }
    public required string ErrorCode { get; init; }

    public static CleanseReportCsvRecord FromDomain(CleanseReportItem item) => new()
    {
        CPH = item.Cph,
        ErrorCode = item.Code
    };
}

/// <summary>
/// CsvHelper mapping configuration for CleanseReportCsvRecord.
/// </summary>
public sealed class CleanseReportCsvMap : ClassMap<CleanseReportCsvRecord>
{
    public CleanseReportCsvMap()
    {
        Map(m => m.CPH).Index(0).Name("CPH");
        Map(m => m.ErrorCode).Index(1).Name("ErrorCode");
    }
}
