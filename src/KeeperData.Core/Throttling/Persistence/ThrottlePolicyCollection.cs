using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Database;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Core.Throttling.Persistence;

[ExcludeFromCodeCoverage(Justification = "MongoDB collection accessor - covered by integration tests.")]
public sealed class ThrottlePolicyCollection
{
    private const string CollectionName = "throttle_policies";

    internal IMongoCollection<ThrottlePolicyDocument> Collection { get; }

    public ThrottlePolicyCollection(IMongoClient mongoClient, IOptions<IDatabaseConfig> databaseConfig)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        Collection = database.GetCollection<ThrottlePolicyDocument>(CollectionName);
    }
}
