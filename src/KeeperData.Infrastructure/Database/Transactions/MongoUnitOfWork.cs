using KeeperData.Core.Transactions;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Database.Transactions;

public class MongoUnitOfWork(IClientSessionHandle session) : IUnitOfWork, ITransactionManager
{
    public IClientSessionHandle Session { get; } = session;

    public void BeginTransactionAsync() => Session.StartTransaction();
    public async Task CommitTransactionAsync() => await Session.CommitTransactionAsync();
    public async Task AbortTransactionAsync() => await Session.AbortTransactionAsync();

    public async Task CommitAsync() => await CommitTransactionAsync();
    public async Task RollbackAsync() => await AbortTransactionAsync();
}