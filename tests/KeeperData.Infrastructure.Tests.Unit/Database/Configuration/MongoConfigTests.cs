using FluentAssertions;
using KeeperData.Core.Database;
using KeeperData.Infrastructure.Database.Configuration;

namespace KeeperData.Infrastructure.Tests.Unit.Database.Configuration;

public class MongoConfigTests
{
    [Fact]
    public void ImplementsIDatabaseConfig()
    {
        var config = new MongoConfig();

        config.Should().BeAssignableTo<IDatabaseConfig>();
    }

    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var config = new MongoConfig();

        config.DatabaseUri.Should().BeNull();
        config.DatabaseName.Should().BeNull();
        config.EnableTransactions.Should().BeFalse();
        config.HealthcheckEnabled.Should().BeTrue();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var config = new MongoConfig
        {
            DatabaseUri = "mongodb://localhost:27017",
            DatabaseName = "keeper_data",
            EnableTransactions = true,
            HealthcheckEnabled = false
        };

        config.DatabaseUri.Should().Be("mongodb://localhost:27017");
        config.DatabaseName.Should().Be("keeper_data");
        config.EnableTransactions.Should().BeTrue();
        config.HealthcheckEnabled.Should().BeFalse();
    }

    [Fact]
    public void Record_SupportsWithExpression()
    {
        var original = new MongoConfig
        {
            DatabaseUri = "mongodb://localhost:27017",
            DatabaseName = "original_db"
        };

        var modified = original with { DatabaseName = "modified_db", EnableTransactions = true };

        modified.DatabaseUri.Should().Be("mongodb://localhost:27017");
        modified.DatabaseName.Should().Be("modified_db");
        modified.EnableTransactions.Should().BeTrue();
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var config1 = new MongoConfig { DatabaseUri = "mongodb://localhost", DatabaseName = "db1" };
        var config2 = new MongoConfig { DatabaseUri = "mongodb://localhost", DatabaseName = "db1" };
        var config3 = new MongoConfig { DatabaseUri = "mongodb://localhost", DatabaseName = "db2" };

        config1.Should().Be(config2);
        config1.Should().NotBe(config3);
    }
}
