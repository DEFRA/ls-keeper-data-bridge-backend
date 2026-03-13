using KeeperData.Core.Reports.Cleanse.Export.Operations.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Dtos;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Export.Operations;

/// <summary>
/// MongoDB query service for ad-hoc cleanse export operations.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB query class - covered by integration tests.")]
public class CleanseExportOperationQueries(CleanseExportOperationsCollection collection)
    : ICleanseExportOperationQueries
{
    private readonly IMongoCollection<CleanseExportOperationDocument> _collection = collection.Collection;

    public async Task<CleanseExportOperationDto?> GetOperationAsync(string exportId, CancellationToken ct = default)
    {
        var filter = Builders<CleanseExportOperationDocument>.Filter.Eq(d => d.Id, exportId);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document?.ToDto();
    }

    public async Task<IReadOnlyList<CleanseExportOperationSummaryDto>> GetOperationsAsync(
        int skip, int top, CancellationToken ct = default)
    {
        var documents = await _collection
            .Find(Builders<CleanseExportOperationDocument>.Filter.Empty)
            .SortByDescending(d => d.StartedAtUtc)
            .Skip(skip)
            .Limit(top)
            .ToListAsync(ct);

        return documents.Select(d => d.ToSummaryDto()).ToList();
    }
}
