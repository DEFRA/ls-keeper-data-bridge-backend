using FluentAssertions;
using KeeperData.Infrastructure.Database.Transactions;
using MongoDB.Driver;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Database.Transactions;

public class MongoUnitOfWorkTests
{
    private readonly Mock<IClientSessionHandle> _sessionMock = new();
    private readonly MongoUnitOfWork _unitOfWork;

    public MongoUnitOfWorkTests()
    {
        _unitOfWork = new MongoUnitOfWork(_sessionMock.Object);
    }

    [Fact]
    public void BeginTransactionAsync_CallsStartTransaction()
    {
        _unitOfWork.BeginTransactionAsync();

        _sessionMock.Verify(s => s.StartTransaction(It.IsAny<TransactionOptions>()), Times.Once);
    }

    [Fact]
    public async Task CommitTransactionAsync_CallsCommitTransactionAsync()
    {
        await _unitOfWork.CommitTransactionAsync();

        _sessionMock.Verify(s => s.CommitTransactionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task AbortTransactionAsync_CallsAbortTransactionAsync()
    {
        await _unitOfWork.AbortTransactionAsync();

        _sessionMock.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CommitAsync_DelegatesToCommitTransactionAsync()
    {
        await _unitOfWork.CommitAsync();

        _sessionMock.Verify(s => s.CommitTransactionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RollbackAsync_DelegatesToAbortTransactionAsync()
    {
        await _unitOfWork.RollbackAsync();

        _sessionMock.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public void Session_ReturnsInjectedSession()
    {
        _unitOfWork.Session.Should().BeSameAs(_sessionMock.Object);
    }
}