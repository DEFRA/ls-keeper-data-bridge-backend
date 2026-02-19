using KeeperData.Core.Database;
using KeeperData.Core.Reports.Internal.Documents;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Internal.Collections;

/// <summary>
/// Lightweight accessor for the cleanse analysis operations MongoDB collection.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB collection accessor - covered by integration tests.")]
public class CleanseOperationsCollection
{
    private const string CollectionName = "ca_operations";

    internal IMongoCollection<CleanseAnalysisOperationDocument> Collection { get; }

    public CleanseOperationsCollection(IMongoClient mongoClient, IOptions<IDatabaseConfig> databaseConfig)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        Collection = database.GetCollection<CleanseAnalysisOperationDocument>(CollectionName);
    }
}
