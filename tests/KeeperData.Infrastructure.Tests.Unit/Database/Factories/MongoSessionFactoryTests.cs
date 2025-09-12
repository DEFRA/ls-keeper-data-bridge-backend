using FluentAssertions;
using KeeperData.Infrastructure.Database.Factories.Implementations;
using MongoDB.Driver;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Database.Factories;

public class MongoSessionFactoryTests
{
    private readonly Mock<IMongoClient> _mongoClientMock;
    private readonly Mock<IClientSessionHandle> _clientSessionHandleMock;
    private readonly MongoSessionFactory _sut;

    public MongoSessionFactoryTests()
    {
        _mongoClientMock = new Mock<IMongoClient>();
        _clientSessionHandleMock = new Mock<IClientSessionHandle>();
        _mongoClientMock.Setup(x => x.StartSession(It.IsAny<ClientSessionOptions>(), It.IsAny<CancellationToken>()))
            .Returns(_clientSessionHandleMock.Object);

        _sut = new MongoSessionFactory(_mongoClientMock.Object);
    }

    [Fact]
    public void GivenNoExistingSessionExists_WhenCallingGetSession_ThenShouldStartNewSession()
    {
        var result = _sut.GetSession();

        result.Should().BeEquivalentTo(_clientSessionHandleMock.Object);

        _mongoClientMock.Verify(x => x.StartSession(null, CancellationToken.None), Times.Once);
    }

    [Fact]
    public void GivenExistingSessionExists_WhenCallingGetSession_ThenShouldReturnSameSession()
    {
        var firstCall = _sut.GetSession();

        var secondCall = _sut.GetSession();

        secondCall.Should().NotBeNull();
        secondCall.Should().BeEquivalentTo(firstCall);

        _mongoClientMock.Verify(x => x.StartSession(null, CancellationToken.None), Times.Once);
    }
}