using MongoDB.Driver;

namespace KeeperData.Core.Transactions;

public interface IUnitOfWork
{
    Task CommitAsync();
    Task RollbackAsync();
    IClientSessionHandle Session { get; }
}