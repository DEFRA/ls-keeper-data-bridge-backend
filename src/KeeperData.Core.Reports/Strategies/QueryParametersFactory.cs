using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.Reports.Strategies;

/// <summary>
/// Factory class for creating query parameters.
/// </summary>
internal static class QueryParametersFactory
{
    public static QueryParameters GetSamHerdQuery(DataSetDefinitions dataSetDefinitions, Cph cph) => new()
    {
        CollectionName = dataSetDefinitions.SamHerd.Name,
        Filter = BuildFilter(FilterExpression.StartsWith(DataFields.SamHerd.Cphh, cph.Value)),
        Top = 1
    };

    /// <summary>
    /// Gets the query parameters for English CTS CPH Holdings i.e., WHERE LID_FULL_IDENTIFIER starts with 'AH'.
    /// </summary>
    /// <param name="dataSetDefinitions"></param>
    /// <returns></returns>
    public static QueryParameters GetEnglishCtsCphHoldings(DataSetDefinitions dataSetDefinitions, int skip, int take) => new()
    {
        CollectionName = dataSetDefinitions.CTSCPHHolding.Name,
        Filter = BuildFilter(FilterExpression.StartsWith(DataFields.CtsCphHoldingFields.LidFullIdentifier, "AH-")),
        Skip = skip,
        Top = take,
    };

    public static QueryParameters GetEnglishCtsCphHoldingsCount(DataSetDefinitions dataSetDefinitions) => new()
    {
        CollectionName = dataSetDefinitions.CTSCPHHolding.Name,
        Filter = BuildFilter(FilterExpression.StartsWith(DataFields.CtsCphHoldingFields.LidFullIdentifier, "AH-")),
        Top = 0,
        IncludeCount = true
    };

    /// <summary>
    /// Gets the CTS Keepers for a given LidFullIdentifier.
    /// </summary>
    /// <param name="dataSetDefinitions"></param>
    /// <param name="identifier"></param>
    /// <returns></returns>
    public static QueryParameters GetCtsKeepers(DataSetDefinitions dataSetDefinitions, LidFullIdentifier identifier) => new()
    {
        CollectionName = dataSetDefinitions.CTSKeeper.Name,
        Filter = BuildFilter(FilterExpression.Equal(DataFields.CtsKeeperFields.LidFullIdentifier, identifier.Value)),
        Top = 100,
    };

    public static QueryParameters GetSamPartiesQuery(DataSetDefinitions dataSetDefinitions, string partyId) => new()
    {
        CollectionName = dataSetDefinitions.SamParty.Name,
        Filter = BuildFilter(FilterExpression.Equal(DataFields.SamPartyFields.PartyId, partyId)),
        Top = 100
    };


    public static QueryParameters GetSamCphHolding(DataSetDefinitions dataSetDefinitions, Cph cph) => new()
    {
        CollectionName = dataSetDefinitions.SamCPHHolding.Name,
        Filter = BuildFilter(FilterExpression.Equal(DataFields.SamCphHoldingFields.Cph, cph.Value)),
        Top = 1
    };

    private static FilterExpression BuildFilter(FilterExpression filter)
    {
        return FilterExpression.And(
            filter,
            FilterExpression.Equal(DataFields.IsDeleted, false)
        );
    }
}
