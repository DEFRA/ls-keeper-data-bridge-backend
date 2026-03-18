using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Cleanse.Export.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Dtos;
using KeeperData.Core.Reports.Internal.Documents;

namespace KeeperData.Core.Reports.Internal.Mappers;

/// <summary>
/// Maps between <see cref="CleanseExportOperationDocument"/> and domain/DTO types.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Internal mapper - covered by integration tests.")]
internal static class CleanseExportOperationDocumentMapper
{
    public static CleanseExportOperationDocument ToDocument(this CleanseExportOperation operation) => new()
    {
        Id = operation.Id,
        Status = operation.Status.ToString(),
        StartedAtUtc = operation.StartedAtUtc,
        CompletedAtUtc = operation.CompletedAtUtc,
        ProgressPercentage = operation.ProgressPercentage,
        StatusDescription = operation.StatusDescription,
        TotalRecords = operation.TotalRecords,
        RecordsExported = operation.RecordsExported,
        ReportObjectKey = operation.ReportObjectKey,
        ReportUrl = operation.ReportUrl,
        Error = operation.Error,
        DurationMs = operation.DurationMs
    };

    public static CleanseExportOperation ToAggregateRoot(this CleanseExportOperationDocument doc) => new()
    {
        Id = doc.Id,
        Status = Enum.Parse<CleanseExportStatus>(doc.Status),
        StartedAtUtc = doc.StartedAtUtc,
        CompletedAtUtc = doc.CompletedAtUtc,
        ProgressPercentage = doc.ProgressPercentage,
        StatusDescription = doc.StatusDescription,
        TotalRecords = doc.TotalRecords,
        RecordsExported = doc.RecordsExported,
        ReportObjectKey = doc.ReportObjectKey,
        ReportUrl = doc.ReportUrl,
        Error = doc.Error,
        DurationMs = doc.DurationMs
    };

    public static CleanseExportOperationDto ToDto(this CleanseExportOperationDocument doc) => new()
    {
        Id = doc.Id,
        Status = doc.Status,
        StartedAtUtc = doc.StartedAtUtc,
        CompletedAtUtc = doc.CompletedAtUtc,
        ProgressPercentage = doc.ProgressPercentage,
        StatusDescription = doc.StatusDescription,
        TotalRecords = doc.TotalRecords,
        RecordsExported = doc.RecordsExported,
        ReportObjectKey = doc.ReportObjectKey,
        ReportUrl = doc.ReportUrl,
        Error = doc.Error,
        DurationMs = doc.DurationMs
    };

    public static CleanseExportOperationSummaryDto ToSummaryDto(this CleanseExportOperationDocument doc) => new()
    {
        Id = doc.Id,
        Status = doc.Status,
        StartedAtUtc = doc.StartedAtUtc,
        CompletedAtUtc = doc.CompletedAtUtc,
        ProgressPercentage = doc.ProgressPercentage,
        TotalRecords = doc.TotalRecords,
        RecordsExported = doc.RecordsExported,
        ReportObjectKey = doc.ReportObjectKey,
        ReportUrl = doc.ReportUrl,
        DurationMs = doc.DurationMs
    };
}
