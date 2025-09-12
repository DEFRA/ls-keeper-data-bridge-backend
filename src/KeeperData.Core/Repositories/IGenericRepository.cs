using KeeperData.Core.Domain;
using MongoDB.Driver;

namespace KeeperData.Core.Repositories;

public interface IGenericRepository<T> where T : IEntity
{
    Task<T> GetByIdAsync(string id, CancellationToken cancellationToken = default);
    Task AddAsync(T entity, CancellationToken cancellationToken = default);
    Task UpdateAsync(T entity, CancellationToken cancellationToken = default);
    Task BulkUpsertAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);
    Task BulkUpsertWithCustomFilterAsync(IEnumerable<(FilterDefinition<T> Filter, T Entity)> items, CancellationToken cancellationToken = default);
    Task DeleteAsync(string id, CancellationToken cancellationToken = default);
}