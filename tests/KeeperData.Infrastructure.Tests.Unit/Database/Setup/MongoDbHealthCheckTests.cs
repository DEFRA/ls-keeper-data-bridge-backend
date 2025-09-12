using FluentAssertions;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Database.Setup;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Database.Setup;

public class MongoDbHealthCheckTests
{
    private readonly Mock<IMongoClient> _mongoClientMock;
    private readonly Mock<IOptions<MongoConfig>> _mongoConfigMock;
    private readonly HealthCheckContext _healthCheckContext = new();

    private readonly MongoDbHealthCheck _sut;

    public MongoDbHealthCheckTests()
    {
        _mongoClientMock = new Mock<IMongoClient>();

        _mongoClientMock.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>()))
            .Returns(() => new Mock<IMongoDatabase>().Object);

        _mongoConfigMock = new Mock<IOptions<MongoConfig>>();
        _mongoConfigMock.Setup(x => x.Value).Returns(new MongoConfig
        {
            DatabaseName = "database-name",
            DatabaseUri = "mongodb://localhost:27017"
        });

        _sut = new MongoDbHealthCheck(_mongoClientMock.Object, _mongoConfigMock.Object);
    }

    [Fact]
    public async Task GivenValidDatabaseName_WhenCallingCheckHealthAsync_ShouldSucceed()
    {
        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task GivenListTopicsRequestFails_WhenCallingCheckHealthAsync_ShouldFail()
    {
        _mongoClientMock
            .Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>()))
            .Throws(new MongoException("MongoDB connection failure"));

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Be("MongoDB health check failed");
        result.Exception.Should().BeAssignableTo<MongoException>();
    }
}