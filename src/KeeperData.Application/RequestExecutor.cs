using KeeperData.Application.Commands;
using KeeperData.Application.Queries;
using MediatR;

namespace KeeperData.Application;

public class RequestExecutor(IMediator mediator) : IRequestExecutor
{
    private readonly IMediator _mediator = mediator;

    public async Task<TResponse> ExecuteCommand<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default)
    {
        return await _mediator.Send(command, cancellationToken);
    }

    public async Task<TResponse> ExecuteQuery<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default)
    {
        return await _mediator.Send(query, cancellationToken);
    }
}