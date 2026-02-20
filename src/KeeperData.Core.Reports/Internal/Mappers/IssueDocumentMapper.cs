using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Query.Dtos;

namespace KeeperData.Core.Reports.Internal.Mappers;

/// <summary>
/// Maps between <see cref="IssueDocument"/> and domain/DTO types.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Internal mapper - covered by integration tests.")]
internal static class IssueDocumentMapper
{
    public static IssueDocument ToDocument(this Issue issue) => new()
    {
        Id = issue.Id,
        OperationId = issue.OperationId,
        IssueCode = issue.IssueCode,
        RuleCode = issue.RuleCode,
        ErrorCode = issue.ErrorCode,
        ErrorDescription = issue.ErrorDescription,
        CtsLidFullIdentifier = issue.CtsLidFullIdentifier,
        Cph = issue.Cph,
        CreatedAtUtc = issue.CreatedAtUtc,
        LastUpdatedAtUtc = issue.LastUpdatedAtUtc,
        IsActive = issue.IsActive,
        IsIgnored = issue.IsIgnored,
        ResolutionStatus = issue.ResolutionStatus.ToString(),
        AssignedTo = issue.AssignedTo,
        EmailCTS = issue.EmailCTS,
        EmailSAM = issue.EmailSAM,
        TelCTS = issue.TelCTS,
        TelSAM = issue.TelSAM,
        FSA = issue.FSA
    };

    public static Issue ToAggregateRoot(this IssueDocument doc) => new()
    {
        Id = doc.Id,
        OperationId = doc.OperationId,
        IssueCode = doc.IssueCode,
        RuleCode = doc.RuleCode,
        ErrorCode = doc.ErrorCode,
        ErrorDescription = doc.ErrorDescription,
        CtsLidFullIdentifier = doc.CtsLidFullIdentifier,
        Cph = doc.Cph,
        CreatedAtUtc = doc.CreatedAtUtc,
        LastUpdatedAtUtc = doc.LastUpdatedAtUtc,
        IsActive = doc.IsActive,
        IsIgnored = doc.IsIgnored,
        ResolutionStatus = Enum.TryParse<ResolutionStatus>(doc.ResolutionStatus, out var rs) ? rs : ResolutionStatus.None,
        AssignedTo = doc.AssignedTo,
        EmailCTS = doc.EmailCTS,
        EmailSAM = doc.EmailSAM,
        TelCTS = doc.TelCTS,
        TelSAM = doc.TelSAM,
        FSA = doc.FSA
    };

    public static IssueDto ToDto(this IssueDocument doc) => new()
    {
        Id = doc.Id,
        IssueCode = doc.IssueCode,
        RuleCode = doc.RuleCode,
        ErrorCode = doc.ErrorCode,
        ErrorDescription = doc.ErrorDescription,
        CtsLidFullIdentifier = doc.CtsLidFullIdentifier,
        Cph = doc.Cph,
        CreatedAtUtc = doc.CreatedAtUtc,
        LastUpdatedAtUtc = doc.LastUpdatedAtUtc,
        IsActive = doc.IsActive,
        IsIgnored = doc.IsIgnored,
        ResolutionStatus = doc.ResolutionStatus,
        AssignedTo = doc.AssignedTo,
        EmailCTS = doc.EmailCTS,
        EmailSAM = doc.EmailSAM,
        TelCTS = doc.TelCTS,
        TelSAM = doc.TelSAM,
        FSA = doc.FSA
    };
}
