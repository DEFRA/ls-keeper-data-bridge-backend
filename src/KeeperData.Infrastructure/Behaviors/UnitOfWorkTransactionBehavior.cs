using KeeperData.Core.Transactions;
using KeeperData.Infrastructure.Database.Configuration;
using MediatR;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Behaviors;

public class UnitOfWorkTransactionBehavior<TRequest, TResponse>(IOptions<MongoConfig> mongoConfig, IUnitOfWork unitOfWork) : IPipelineBehavior<TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    private readonly IOptions<MongoConfig> _mongoConfig = mongoConfig;
    private readonly IUnitOfWork _unitOfWork = unitOfWork;

    public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken)
    {
        var transactionsEnabled = _mongoConfig.Value.EnableTransactions;
        var transactionStarted = false;

        if (transactionsEnabled && _unitOfWork.Session?.IsInTransaction == false)
        {
            _unitOfWork.Session.StartTransaction();
            transactionStarted = true;
        }

        try
        {
            var response = await next(cancellationToken);

            if (transactionStarted)
            {
                await _unitOfWork.CommitAsync();
            }

            return response;
        }
        catch (Exception)
        {
            if (transactionStarted && _unitOfWork.Session != null && _unitOfWork.Session.IsInTransaction)
            {
                await _unitOfWork.RollbackAsync();
            }

            throw;
        }
    }
}