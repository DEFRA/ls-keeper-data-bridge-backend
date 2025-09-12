using FluentAssertions;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Database.Factories.Implementations;
using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Database.Factories;

public class MongoDbClientFactoryTests
{
    private readonly Mock<IOptions<MongoConfig>> _mongoConfigMock;
    private readonly MongoDbClientFactory _sut;

    public MongoDbClientFactoryTests()
    {
        _mongoConfigMock = new Mock<IOptions<MongoConfig>>();
        _mongoConfigMock.Setup(x => x.Value).Returns(new MongoConfig
        {
            DatabaseName = "database-name",
            DatabaseUri = "mongodb://localhost:27017"
        });

        _sut = new MongoDbClientFactory(_mongoConfigMock.Object);
    }

    [Fact]
    public void GivenMissingMongoConfig_WhenConstructed_ShouldThrow()
    {
        _mongoConfigMock.Setup(x => x.Value).Returns(() => null!);

        Action act = () => _ = new MongoDbClientFactory(_mongoConfigMock.Object);

        act.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData(null, null, "MongoDB uri string cannot be empty")]
    [InlineData("", null, "MongoDB uri string cannot be empty")]
    [InlineData(" ", null, "MongoDB uri string cannot be empty")]
    [InlineData("mongodb://localhost:27017", null, "MongoDB database name cannot be empty")]
    [InlineData("mongodb://localhost:27017", "", "MongoDB database name cannot be empty")]
    [InlineData("mongodb://localhost:27017", " ", "MongoDB database name cannot be empty")]
    public void GivenMissingMongoConfigValues_WhenConstructed_ShouldThrow(string? databaseUri, string? databaseName, string expectedMessage)
    {
        _mongoConfigMock.Setup(x => x.Value).Returns(new MongoConfig
        {
            DatabaseName = databaseName!,
            DatabaseUri = databaseUri!
        });

        Action act = () => _ = new MongoDbClientFactory(_mongoConfigMock.Object);

        act.Should().Throw<ArgumentException>().WithMessage(expectedMessage);
    }

    [Fact]
    public void GivenNoCachedClientExists_WhenCallingCreateClient_ThenShouldReturnNewClient()
    {
        var result = _sut.CreateClient();

        result.Should().NotBeNull();
    }

    [Fact]
    public void GivenCachedClientExists_WhenCallingCreateClient_ThenShouldReturnExistingInstance()
    {
        var firstClient = _sut.CreateClient();
        var secondClient = _sut.CreateClient();

        secondClient.Should().NotBeNull();
        secondClient.Should().BeEquivalentTo(firstClient);
    }
}