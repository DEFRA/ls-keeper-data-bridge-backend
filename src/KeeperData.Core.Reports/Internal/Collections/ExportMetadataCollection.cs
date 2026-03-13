using KeeperData.Core.Database;
using KeeperData.Core.Reports.Internal.Documents;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Internal.Collections;

/// <summary>
/// Lightweight accessor for the export metadata MongoDB collection.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB collection accessor - covered by integration tests.")]
public class ExportMetadataCollection
{
    private const string CollectionName = "ca_export_metadata";

    internal IMongoCollection<ExportMetadataDocument> Collection { get; }

    public ExportMetadataCollection(IMongoClient mongoClient, IOptions<IDatabaseConfig> databaseConfig)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        Collection = database.GetCollection<ExportMetadataDocument>(CollectionName);
    }
}
