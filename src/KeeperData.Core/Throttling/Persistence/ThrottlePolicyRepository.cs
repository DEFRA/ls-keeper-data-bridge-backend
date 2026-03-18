using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Models;
using MongoDB.Driver;

namespace KeeperData.Core.Throttling.Persistence;

[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public sealed class ThrottlePolicyRepository(ThrottlePolicyCollection collection) : IThrottlePolicyRepository
{
    private readonly IMongoCollection<ThrottlePolicyDocument> _col = collection.Collection;

    public async Task<IReadOnlyList<ThrottlePolicy>> GetAllAsync(CancellationToken ct = default)
    {
        var docs = await _col.Find(FilterDefinition<ThrottlePolicyDocument>.Empty).ToListAsync(ct);
        return docs.Select(d => d.ToModel()).ToList();
    }

    public async Task<ThrottlePolicy?> GetBySlugAsync(string slug, CancellationToken ct = default)
    {
        var doc = await _col.Find(d => d.Slug == slug).FirstOrDefaultAsync(ct);
        return doc?.ToModel();
    }

    public async Task<ThrottlePolicy?> GetActiveAsync(CancellationToken ct = default)
    {
        var doc = await _col.Find(d => d.IsActive).FirstOrDefaultAsync(ct);
        return doc?.ToModel();
    }

    public async Task UpsertAsync(ThrottlePolicy policy, CancellationToken ct = default)
    {
        var doc = ThrottlePolicyDocument.FromModel(policy);
        var filter = Builders<ThrottlePolicyDocument>.Filter.Eq(d => d.Slug, policy.Slug);
        await _col.ReplaceOneAsync(filter, doc, new ReplaceOptions { IsUpsert = true }, ct);
    }

    public async Task<bool> DeleteAsync(string slug, CancellationToken ct = default)
    {
        var result = await _col.DeleteOneAsync(d => d.Slug == slug, ct);
        return result.DeletedCount > 0;
    }

    public async Task DeactivateAllAsync(CancellationToken ct = default)
    {
        var filter = Builders<ThrottlePolicyDocument>.Filter.Eq(d => d.IsActive, true);
        var update = Builders<ThrottlePolicyDocument>.Update.Set(d => d.IsActive, false);
        await _col.UpdateManyAsync(filter, update, cancellationToken: ct);
    }

    public async Task<long> CountAsync(CancellationToken ct = default)
    {
        return await _col.CountDocumentsAsync(FilterDefinition<ThrottlePolicyDocument>.Empty, cancellationToken: ct);
    }
}
