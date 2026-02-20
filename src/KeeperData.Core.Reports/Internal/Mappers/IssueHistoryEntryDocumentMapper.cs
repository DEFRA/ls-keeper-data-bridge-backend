using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Query.Dtos;

namespace KeeperData.Core.Reports.Internal.Mappers;

/// <summary>
/// Maps between <see cref="IssueHistoryEntryDocument"/> and domain/DTO types.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Internal mapper - covered by integration tests.")]
internal static class IssueHistoryEntryDocumentMapper
{
    public static IssueHistoryEntryDocument ToDocument(this IssueHistoryEntry entry) => new()
    {
        Id = entry.Id,
        IssueId = entry.IssueId,
        Action = entry.Action.ToString(),
        PerformedBy = entry.PerformedBy,
        Detail = entry.Detail,
        OccurredAtUtc = entry.OccurredAtUtc
    };

    public static IssueHistoryEntryDto ToDto(this IssueHistoryEntryDocument doc) => new()
    {
        Id = doc.Id,
        IssueId = doc.IssueId,
        Action = doc.Action,
        PerformedBy = doc.PerformedBy,
        Detail = doc.Detail,
        OccurredAtUtc = doc.OccurredAtUtc
    };
}
