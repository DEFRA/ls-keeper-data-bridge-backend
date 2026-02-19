using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

namespace KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;

public interface ICtsSamQueryService
{
    Task<CtsCphHoldingModel?> GetCtsCphHoldingAsync(LidFullIdentifier lidFullIdentifier, CancellationToken ct);
    Task<CtsCphHoldingModel?> GetCtsCphHoldingAsync(Cph cph, CancellationToken ct);
    Task<int> GetCtsCphHoldingsCountAsync(CancellationToken ct);
    Task<SamCphHoldingModel?> GetSamCphHoldingAsync(Cph cph, CancellationToken ct);
    Task<int> GetSamCphHoldingsCountAsync(CancellationToken ct);
    Task<QueryResult> ListCtsCphHoldingsAsync(int skip, int take, CancellationToken ct);
    Task<QueryResult> ListSamCphHoldingsAsync(int skip, int take, CancellationToken ct);
}
