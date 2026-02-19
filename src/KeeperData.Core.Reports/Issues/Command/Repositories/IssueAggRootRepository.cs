using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Issues.Command.Repositories;

/// <summary>
/// MongoDB repository for Issue aggregate root persistence.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class IssueAggRootRepository(IssueCollection issueCollection) : IIssueAggRootRepository
{
    private readonly IMongoCollection<IssueDocument> _collection = issueCollection.Collection;

    public async Task<Issue?> GetByIdAsync(string id, CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.Eq(d => d.Id, id);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document?.ToAggregateRoot();
    }

    public async Task UpsertAsync(Issue item, CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.Eq(d => d.Id, item.Id);
        var options = new ReplaceOptions { IsUpsert = true };
        await _collection.ReplaceOneAsync(filter, item.ToDocument(), options, ct);
    }

    public async Task<int> DeactivateStaleAsync(string currentOperationId, CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.And(
            Builders<IssueDocument>.Filter.Ne(d => d.OperationId, currentOperationId),
            Builders<IssueDocument>.Filter.Eq(d => d.IsActive, true));
        var update = Builders<IssueDocument>.Update
            .Set(d => d.IsActive, false)
            .Set(d => d.LastUpdatedAtUtc, DateTime.UtcNow);
        var result = await _collection.UpdateManyAsync(filter, update, cancellationToken: ct);
        return (int)result.ModifiedCount;
    }

    public async Task<long> DeleteAllAsync(CancellationToken ct = default)
    {
        var result = await _collection.DeleteManyAsync(Builders<IssueDocument>.Filter.Empty, ct);
        return result.DeletedCount;
    }
}
