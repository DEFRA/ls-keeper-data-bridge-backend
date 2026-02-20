using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Issues.Command.Repositories;

/// <summary>
/// MongoDB write repository for issue history/lineage entries.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class IssueHistoryAggRootRepository(IssueHistoryCollection historyCollection) : IIssueHistoryAggRootRepository
{
    private readonly IMongoCollection<IssueHistoryEntryDocument> _collection = historyCollection.Collection;

    public async Task AppendAsync(IssueHistoryEntry entry, CancellationToken ct = default)
    {
        await _collection.InsertOneAsync(entry.ToDocument(), cancellationToken: ct);
    }

    public async Task AppendBatchAsync(IReadOnlyList<IssueHistoryEntry> entries, CancellationToken ct = default)
    {
        if (entries.Count == 0) return;
        var documents = entries.Select(e => e.ToDocument()).ToList();
        await _collection.InsertManyAsync(documents, cancellationToken: ct);
    }
}
