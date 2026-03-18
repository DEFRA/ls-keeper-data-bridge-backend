using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;
using KeeperData.Core.Reports.Internal.Documents;

namespace KeeperData.Core.Reports.Internal.Mappers;

/// <summary>
/// Maps between <see cref="CleanseAnalysisOperationDocument"/> and domain/DTO types.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Internal mapper - covered by integration tests.")]
internal static class CleanseAnalysisOperationDocumentMapper
{
    public static CleanseAnalysisOperationDocument ToDocument(this CleanseAnalysisOperation operation) => new()
    {
        Id = operation.Id,
        Status = operation.Status.ToString(),
        StartedAtUtc = operation.StartedAtUtc,
        CompletedAtUtc = operation.CompletedAtUtc,
        ProgressPercentage = operation.ProgressPercentage,
        StatusDescription = operation.StatusDescription,
        RecordsAnalyzed = operation.RecordsAnalyzed,
        TotalRecords = operation.TotalRecords,
        IssuesFound = operation.IssuesFound,
        IssuesResolved = operation.IssuesResolved,
        Error = operation.Error,
        DurationMs = operation.DurationMs,
        ReportObjectKey = operation.ReportObjectKey,
        ReportUrl = operation.ReportUrl,
        FinalAverageRpm = operation.FinalAverageRpm,
        CancellationRequested = operation.CancellationRequested,
        CancelledAtUtc = operation.CancelledAtUtc,
        CurrentPhase = operation.CurrentPhase,
        Phases = operation.Phases.Select(ToPhaseDocument).ToList()
    };

    public static CleanseAnalysisOperation ToAggregateRoot(this CleanseAnalysisOperationDocument doc) => new()
    {
        Id = doc.Id,
        Status = Enum.Parse<CleanseAnalysisStatus>(doc.Status),
        StartedAtUtc = doc.StartedAtUtc,
        CompletedAtUtc = doc.CompletedAtUtc,
        ProgressPercentage = doc.ProgressPercentage,
        StatusDescription = doc.StatusDescription,
        RecordsAnalyzed = doc.RecordsAnalyzed,
        TotalRecords = doc.TotalRecords,
        IssuesFound = doc.IssuesFound,
        IssuesResolved = doc.IssuesResolved,
        Error = doc.Error,
        DurationMs = doc.DurationMs,
        ReportObjectKey = doc.ReportObjectKey,
        ReportUrl = doc.ReportUrl,
        FinalAverageRpm = doc.FinalAverageRpm,
        CancellationRequested = doc.CancellationRequested,
        CancelledAtUtc = doc.CancelledAtUtc,
        CurrentPhase = doc.CurrentPhase,
        Phases = doc.Phases.Select(ToPhaseProgress).ToList()
    };

    public static CleanseAnalysisOperationDto ToDto(this CleanseAnalysisOperationDocument doc) => new()
    {
        Id = doc.Id,
        Status = Enum.Parse<CleanseAnalysisStatus>(doc.Status),
        StartedAtUtc = doc.StartedAtUtc,
        CompletedAtUtc = doc.CompletedAtUtc,
        ProgressPercentage = doc.ProgressPercentage,
        StatusDescription = doc.StatusDescription,
        RecordsAnalyzed = doc.RecordsAnalyzed,
        TotalRecords = doc.TotalRecords,
        IssuesFound = doc.IssuesFound,
        IssuesResolved = doc.IssuesResolved,
        Error = doc.Error,
        DurationMs = doc.DurationMs,
        ReportObjectKey = doc.ReportObjectKey,
        ReportUrl = doc.ReportUrl,
        FinalAverageRpm = doc.FinalAverageRpm,
        CancelledAtUtc = doc.CancelledAtUtc,
        CurrentPhase = doc.CurrentPhase,
        Phases = doc.Phases.Count > 0 ? doc.Phases.Select(ToPhaseProgress).ToList() : null
    };

    public static CleanseAnalysisOperationSummaryDto ToSummaryDto(this CleanseAnalysisOperationDocument doc) => new()
    {
        Id = doc.Id,
        Status = doc.Status,
        StartedAtUtc = doc.StartedAtUtc,
        CompletedAtUtc = doc.CompletedAtUtc,
        ProgressPercentage = doc.ProgressPercentage,
        RecordsAnalyzed = doc.RecordsAnalyzed,
        TotalRecords = doc.TotalRecords,
        IssuesFound = doc.IssuesFound,
        IssuesResolved = doc.IssuesResolved,
        DurationMs = doc.DurationMs,
        ReportObjectKey = doc.ReportObjectKey,
        ReportUrl = doc.ReportUrl,
        FinalAverageRpm = doc.FinalAverageRpm,
        CancelledAtUtc = doc.CancelledAtUtc,
        CurrentPhase = doc.CurrentPhase
    };

    private static OperationPhaseProgressDocument ToPhaseDocument(OperationPhaseProgress p) => new()
    {
        Name = p.Name,
        Status = p.Status,
        Percentage = p.Percentage,
        Description = p.Description,
        RecordsProcessed = p.RecordsProcessed,
        TotalRecords = p.TotalRecords,
        StartedAtUtc = p.StartedAtUtc,
        CompletedAtUtc = p.CompletedAtUtc,
        DurationMs = p.DurationMs
    };

    private static OperationPhaseProgress ToPhaseProgress(OperationPhaseProgressDocument d) => new()
    {
        Name = d.Name,
        Status = d.Status,
        Percentage = d.Percentage,
        Description = d.Description,
        RecordsProcessed = d.RecordsProcessed,
        TotalRecords = d.TotalRecords,
        StartedAtUtc = d.StartedAtUtc,
        CompletedAtUtc = d.CompletedAtUtc,
        DurationMs = d.DurationMs
    };
}
