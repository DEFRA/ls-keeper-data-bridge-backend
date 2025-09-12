using KeeperData.Application.Commands;
using KeeperData.Application.Queries;

namespace KeeperData.Application;

public interface IRequestExecutor
{
    Task<TResponse> ExecuteCommand<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default);
    Task<TResponse> ExecuteQuery<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default);
}