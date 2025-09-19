using MediatR;

namespace KeeperData.Application.Commands;

public interface ICommand<TResponse> : IRequest<TResponse> { }