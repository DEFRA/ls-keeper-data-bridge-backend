using KeeperData.Core.Reports.Cleanse.Export.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Abstract;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Export.Operations.Repositories;

/// <summary>
/// MongoDB repository for <see cref="CleanseExportOperation"/> aggregate root persistence.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class CleanseExportOperationRepository(CleanseExportOperationsCollection collection)
    : ICleanseExportOperationRepository
{
    private readonly IMongoCollection<CleanseExportOperationDocument> _collection = collection.Collection;

    public async Task CreateAsync(CleanseExportOperation operation, CancellationToken ct = default)
    {
        await _collection.InsertOneAsync(operation.ToDocument(), cancellationToken: ct);
    }

    public async Task<CleanseExportOperation?> GetByIdAsync(string operationId, CancellationToken ct = default)
    {
        var filter = Builders<CleanseExportOperationDocument>.Filter.Eq(d => d.Id, operationId);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document?.ToAggregateRoot();
    }

    public async Task UpdateAsync(CleanseExportOperation operation, CancellationToken ct = default)
    {
        var filter = Builders<CleanseExportOperationDocument>.Filter.Eq(d => d.Id, operation.Id);
        await _collection.ReplaceOneAsync(filter, operation.ToDocument(), cancellationToken: ct);
    }
}
