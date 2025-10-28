using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Database.Setup;

public class MongoDbHealthCheck(IMongoClient mongoClient, IOptions<MongoConfig> mongoConfig) : IHealthCheck
{
    private readonly IMongoClient _mongoClient = mongoClient;
    private readonly MongoConfig _mongoConfig = mongoConfig.Value;
    private static readonly BsonDocumentCommand<BsonDocument> s_command = new(BsonDocument.Parse("{ping:1}"));

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _mongoClient.GetDatabase(_mongoConfig.DatabaseName)
                .RunCommandAsync(s_command, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return HealthCheckResult.Healthy("MongoDB is reachable");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("MongoDB health check failed", ex);
        }
    }
}