using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Operations;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

namespace KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;

/// <summary>
/// Provides pre-loaded, in-memory access to CTS and SAM data for the cleanse analysis pipeline.
/// All lookup methods are synchronous because data is loaded into memory upfront via <see cref="PreloadAsync"/>.
/// </summary>
public interface IPreloadedCtsSamDataService
{
    /// <summary>
    /// Loads all CTS and SAM data into memory. Must be called once before any lookup methods are used.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <param name="scope">Optional operation scope for unified progress tracking.</param>
    Task PreloadAsync(CancellationToken ct, OperationScope? scope = null);

    CtsCphHoldingModel? GetCtsCphHolding(LidFullIdentifier lidFullIdentifier);
    CtsCphHoldingModel? GetCtsCphHolding(Cph cph);
    int GetCtsCphHoldingsCount();

    SamCphHoldingModel? GetSamCphHolding(Cph cph);
    int GetSamCphHoldingsCount();

    QueryResult ListCtsCphHoldings(int skip, int take);
    QueryResult ListSamCphHoldings(int skip, int take);
}
