using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Repositories;

/// <summary>
/// MongoDB repository for CleanseAnalysisOperation aggregate root persistence.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class CleanseAnalysisOperationAggRootRepository(CleanseOperationsCollection operationsCollection)
    : ICleanseAnalysisOperationAggRootRepository
{
    private readonly IMongoCollection<CleanseAnalysisOperationDocument> _collection = operationsCollection.Collection;

    public async Task CreateAsync(CleanseAnalysisOperation operation, CancellationToken ct = default)
    {
        await _collection.InsertOneAsync(operation.ToDocument(), cancellationToken: ct);
    }

    public async Task<CleanseAnalysisOperation?> GetByIdAsync(string operationId, CancellationToken ct = default)
    {
        var filter = Builders<CleanseAnalysisOperationDocument>.Filter.Eq(d => d.Id, operationId);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document?.ToAggregateRoot();
    }

    public async Task UpdateAsync(CleanseAnalysisOperation operation, CancellationToken ct = default)
    {
        var filter = Builders<CleanseAnalysisOperationDocument>.Filter.Eq(d => d.Id, operation.Id);
        await _collection.ReplaceOneAsync(filter, operation.ToDocument(), cancellationToken: ct);
    }

    public async Task<long> DeleteAllAsync(CancellationToken ct = default)
    {
        var result = await _collection.DeleteManyAsync(Builders<CleanseAnalysisOperationDocument>.Filter.Empty, ct);
        return result.DeletedCount;
    }
}
