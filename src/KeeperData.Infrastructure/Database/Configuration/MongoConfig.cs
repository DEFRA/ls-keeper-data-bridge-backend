using KeeperData.Core.Database;

namespace KeeperData.Infrastructure.Database.Configuration;

public record MongoConfig : IDatabaseConfig
{
    public string DatabaseUri { get; init; } = default!;
    public string DatabaseName { get; init; } = default!;
    public bool EnableTransactions { get; init; } = false;
    public bool HealthcheckEnabled { get; init; } = true;
}