using KeeperData.Core.Database;
using KeeperData.Core.Reports.Internal.Documents;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Internal.Collections;

/// <summary>
/// Lightweight accessor for the issue history MongoDB collection.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB collection accessor - covered by integration tests.")]
public class IssueHistoryCollection
{
    private const string CollectionName = "cleanse_issue_history";

    internal IMongoCollection<IssueHistoryEntryDocument> Collection { get; }

    public IssueHistoryCollection(IMongoClient mongoClient, IOptions<IDatabaseConfig> databaseConfig)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        Collection = database.GetCollection<IssueHistoryEntryDocument>(CollectionName);
    }
}
