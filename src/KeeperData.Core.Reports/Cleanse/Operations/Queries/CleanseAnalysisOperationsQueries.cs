using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries;

public class CleanseAnalysisOperationsQueries(CleanseOperationsCollection operationsCollection)
    : ICleanseAnalysisOperationsQueries
{
    private readonly IMongoCollection<CleanseAnalysisOperationDocument> _collection = operationsCollection.Collection;

    public async Task<CleanseAnalysisOperationDto?> GetOperationAsync(string operationId, CancellationToken ct = default)
    {
        var filter = Builders<CleanseAnalysisOperationDocument>.Filter.Eq(d => d.Id, operationId);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document?.ToDto();
    }

    public async Task<IReadOnlyList<CleanseAnalysisOperationSummaryDto>> GetOperationsAsync(int skip, int top, CancellationToken ct = default)
    {
        var documents = await _collection
            .Find(Builders<CleanseAnalysisOperationDocument>.Filter.Empty)
            .SortByDescending(d => d.StartedAtUtc)
            .Skip(skip)
            .Limit(top)
            .ToListAsync(ct);
        return documents.Select(d => d.ToSummaryDto()).ToList();
    }

    public async Task<CleanseAnalysisOperationDto?> GetCurrentOperationAsync(CancellationToken ct = default)
    {
        var filter = Builders<CleanseAnalysisOperationDocument>.Filter.Eq(d => d.Status, CleanseAnalysisStatus.Running.ToString());
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document?.ToDto();
    }
}

