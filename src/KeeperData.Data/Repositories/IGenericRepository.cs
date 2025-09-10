namespace KeeperData.Data.Repositories;

public interface IGenericRepository<T> where T : IEntity
{
    Task<T> GetByIdAsync(string id, CancellationToken cancellationToken = default);
    Task AddAsync(T entity, CancellationToken cancellationToken = default);
    Task UpdateAsync(T entity, CancellationToken cancellationToken = default);
    Task DeleteAsync(string id, CancellationToken cancellationToken = default);
}