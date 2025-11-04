using KeeperData.Core.Reporting.Domain;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace KeeperData.Core.Reporting.Services;

/// <summary>
/// Factory for creating LineageIndexManager instances.
/// Follows Factory Pattern - centralizes object creation logic.
/// </summary>
public interface ILineageIndexManagerFactory
{
    /// <summary>
    /// Creates a LineageIndexManager for the given collection.
    /// </summary>
    ILineageIndexManager Create(IMongoCollection<LineageEventDocument> collection);
}

/// <summary>
/// Default implementation of the LineageIndexManager factory.
/// </summary>
public class LineageIndexManagerFactory : ILineageIndexManagerFactory
{
    private readonly ILoggerFactory _loggerFactory;

    public LineageIndexManagerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    }

    public ILineageIndexManager Create(IMongoCollection<LineageEventDocument> collection)
    {
        if (collection == null) throw new ArgumentNullException(nameof(collection));
        
        var logger = _loggerFactory.CreateLogger<LineageIndexManager>();
        return new LineageIndexManager(collection, logger);
    }
}
