using KeeperData.Core.Reports.Cleanse.Export.Metadata.Abstract;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Export.Metadata;

/// <summary>
/// MongoDB implementation of <see cref="IExportMetadataRepository"/>.
/// Uses a singleton document pattern (single document with fixed _id).
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class ExportMetadataRepository(ExportMetadataCollection metadataCollection) : IExportMetadataRepository
{
    private const string SingletonId = "singleton";
    private readonly IMongoCollection<ExportMetadataDocument> _collection = metadataCollection.Collection;

    public async Task<DateTime?> GetLastExportedAtUtcAsync(CancellationToken ct = default)
    {
        var filter = Builders<ExportMetadataDocument>.Filter.Eq(d => d.Id, SingletonId);
        var doc = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return doc?.LastExportedAtUtc;
    }

    public async Task SetLastExportedAtUtcAsync(DateTime exportedAtUtc, CancellationToken ct = default)
    {
        var filter = Builders<ExportMetadataDocument>.Filter.Eq(d => d.Id, SingletonId);
        var update = Builders<ExportMetadataDocument>.Update.Set(d => d.LastExportedAtUtc, exportedAtUtc);
        await _collection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, ct);
    }
}
