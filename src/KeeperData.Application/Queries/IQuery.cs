using MediatR;

namespace KeeperData.Application.Queries;

public interface IQuery<TResponse> : IRequest<TResponse> { }