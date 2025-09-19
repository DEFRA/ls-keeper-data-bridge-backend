using MediatR;

namespace KeeperData.Application.Queries;

public interface IQueryHandler<in TQuery, TResult> :
IRequestHandler<TQuery, TResult> where TQuery : IQuery<TResult>
{
}