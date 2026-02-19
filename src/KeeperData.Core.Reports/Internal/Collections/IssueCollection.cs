using KeeperData.Core.Database;
using KeeperData.Core.Reports.Internal.Documents;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Internal.Collections;

/// <summary>
/// Lightweight accessor for the issues MongoDB collection.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB collection accessor - covered by integration tests.")]
public class IssueCollection
{
    private const string CollectionName = "issues";

    internal IMongoCollection<IssueDocument> Collection { get; }

    public IssueCollection(IMongoClient mongoClient, IOptions<IDatabaseConfig> databaseConfig)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        Collection = database.GetCollection<IssueDocument>(CollectionName);
    }
}
