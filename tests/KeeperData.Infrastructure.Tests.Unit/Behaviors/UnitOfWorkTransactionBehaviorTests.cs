using FluentAssertions;
using KeeperData.Core.Transactions;
using KeeperData.Infrastructure.Behaviors;
using KeeperData.Infrastructure.Database.Configuration;
using MediatR;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Behaviors;

public class UnitOfWorkTransactionBehaviorTests
{
    private readonly Mock<IClientSessionHandle> _sessionMock = new();
    private readonly Mock<IUnitOfWork> _unitOfWorkMock = new();
    private readonly IOptions<MongoConfig> _mongoConfig;

    public UnitOfWorkTransactionBehaviorTests()
    {
        _mongoConfig = Options.Create(new MongoConfig { EnableTransactions = true });
        _unitOfWorkMock.SetupGet(u => u.Session).Returns(_sessionMock.Object);
    }

    [Fact]
    public async Task GivenTransactionsEnabledAndNotInTransaction_WhenHandled_ThenStartsAndCommitsTransaction()
    {
        _sessionMock.SetupGet(s => s.IsInTransaction).Returns(false);
        var behavior = new UnitOfWorkTransactionBehavior<TransactionTestRequest, string>(_mongoConfig, _unitOfWorkMock.Object);

        static Task<string> next(CancellationToken token = default) => Task.FromResult("Success");

        var result = await behavior.Handle(new TransactionTestRequest(), next, CancellationToken.None);

        result.Should().Be("Success");
        _sessionMock.Verify(s => s.StartTransaction(It.IsAny<TransactionOptions>()), Times.Once);
        _unitOfWorkMock.Verify(u => u.CommitAsync(), Times.Once);
        _unitOfWorkMock.Verify(u => u.RollbackAsync(), Times.Never);
    }

    [Fact]
    public async Task GivenTransactionsDisabled_WhenHandled_ThenDoesNotStartOrCommitTransaction()
    {
        var config = Options.Create(new MongoConfig { EnableTransactions = false });
        var behavior = new UnitOfWorkTransactionBehavior<TransactionTestRequest, string>(config, _unitOfWorkMock.Object);

        static Task<string> next(CancellationToken token = default) => Task.FromResult("Success");

        var result = await behavior.Handle(new TransactionTestRequest(), next, CancellationToken.None);

        result.Should().Be("Success");
        _sessionMock.Verify(s => s.StartTransaction(It.IsAny<TransactionOptions>()), Times.Never);
        _unitOfWorkMock.Verify(u => u.CommitAsync(), Times.Never);
        _unitOfWorkMock.Verify(u => u.RollbackAsync(), Times.Never);
    }

    [Fact]
    public async Task GivenExceptionDuringHandler_WhenHandled_ThenRollsBackTransaction()
    {
        _sessionMock.SetupSequence(s => s.IsInTransaction)
            .Returns(false)
            .Returns(true);

        var behavior = new UnitOfWorkTransactionBehavior<TransactionTestRequest, string>(_mongoConfig, _unitOfWorkMock.Object);

        static Task<string> next(CancellationToken token = default) => throw new InvalidOperationException("Boom");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            behavior.Handle(new TransactionTestRequest(), next, CancellationToken.None));

        ex.Message.Should().Be("Boom");
        _sessionMock.Verify(s => s.StartTransaction(It.IsAny<TransactionOptions>()), Times.Once);
        _unitOfWorkMock.Verify(u => u.CommitAsync(), Times.Never);
        _unitOfWorkMock.Verify(u => u.RollbackAsync(), Times.Once);
    }

    [Fact]
    public async Task GivenAlreadyInTransaction_WhenHandled_ThenDoesNotStartNewTransaction()
    {
        _sessionMock.SetupGet(s => s.IsInTransaction).Returns(true);
        var behavior = new UnitOfWorkTransactionBehavior<TransactionTestRequest, string>(_mongoConfig, _unitOfWorkMock.Object);

        static Task<string> next(CancellationToken token = default) => Task.FromResult("Success");

        var result = await behavior.Handle(new TransactionTestRequest(), next, CancellationToken.None);

        result.Should().Be("Success");
        _sessionMock.Verify(s => s.StartTransaction(It.IsAny<TransactionOptions>()), Times.Never);
        _unitOfWorkMock.Verify(u => u.CommitAsync(), Times.Never);
        _unitOfWorkMock.Verify(u => u.RollbackAsync(), Times.Never);
    }
}

public class TransactionTestRequest : IRequest<string>
{
    public string Payload { get; set; } = string.Empty;
}