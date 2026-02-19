using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using static KeeperData.Core.Reports.SamCtsHoldings.Query.Domain.DataFields;

namespace KeeperData.Core.Reports.SamCtsHoldings.Query;

public class CtsSamQueryService(DataSetDefinitions dataSetDefinitions, IQueryService queryService) : ICtsSamQueryService
{
    public Task<QueryResult> ListCtsCphHoldingsAsync(int skip, int take, CancellationToken ct)
    {
        var query = GetCtsCphHoldingsQuery(skip, take);
        return queryService.QueryAsync(query, ct);
    }

    public Task<QueryResult> ListSamCphHoldingsAsync(int skip, int take, CancellationToken ct)
    {
        var query = GetSamCphHoldingsQuery(skip, take);
        return queryService.QueryAsync(query, ct);
    }


    public async Task<int> GetCtsCphHoldingsCountAsync(CancellationToken ct)
    {
        var query = GetCtsCphHoldingsCountQuery();
        var result = await queryService.QueryAsync(query, ct);
        return (int)(result.TotalCount ?? 0);
    }

    public async Task<int> GetSamCphHoldingsCountAsync(CancellationToken ct)
    {
        var query = GetSamCphHoldingsCountQuery();
        var result = await queryService.QueryAsync(query, ct);
        return (int)(result.TotalCount ?? 0);
    }


    public async Task<CtsCphHoldingModel?> GetCtsCphHoldingAsync(LidFullIdentifier lidFullIdentifier, CancellationToken ct)
    {
        var holdingQuery = new QueryParameters()
        {
            CollectionName = dataSetDefinitions.CTSCPHHolding.Name,
            Filter = BuildFilter(FilterExpression.Equal(CtsKeeperFields.LidFullIdentifier, lidFullIdentifier.Value)),
            Skip = 0,
            Top = 1,
        };
        return await GetCtsCphHoldingAsync(holdingQuery, ct);
    }

    public async Task<CtsCphHoldingModel?> GetCtsCphHoldingAsync(Cph cph, CancellationToken ct)
    {
        var holdingQuery = GetCtsCphHoldingByCphQuery(cph);
        return await GetCtsCphHoldingAsync(holdingQuery, ct);
    }

    private async Task<CtsCphHoldingModel?> GetCtsCphHoldingAsync(QueryParameters holdingQuery, CancellationToken ct)
    {
        var holding = (await queryService.QueryAsync(holdingQuery)).Data.ElementAtOrDefault(0);
        var lidFullIdentifier = LidFullIdentifier.TryParse(holding?[CtsCphHoldingFields.LidFullIdentifier]?.ToString());
        if (holding == null || lidFullIdentifier == null)
        {
            return null;
        }
        else
        {   
            var keepersQuery = GetCtsKeepersQuery(lidFullIdentifier);
            var keepers = await queryService.QueryAsync(keepersQuery);
            return new CtsCphHoldingModel
            {
                Id = lidFullIdentifier,
                Holding = holding,
                Keepers = keepers
            };
        }
    }


    public async Task<SamCphHoldingModel?> GetSamCphHoldingAsync(Cph cph, CancellationToken ct)
    {
        var holdingQuery = GetSamCphHoldingQuery(cph);
        var holding = (await queryService.QueryAsync(holdingQuery, ct)).Data.ElementAtOrDefault(0);
        if (holding == null)
        {
            return null;
        }
        else
        {
            var samHerdsQuery = GetSamHerdQuery(cph);
            var samHerds = await queryService.QueryAsync(samHerdsQuery, ct);
            var partyIds = samHerds.Data
                .SelectMany(x => new[]
                {
                    x[SamHerd.OwnerPartyIds]?.ToString(),
                    x[SamHerd.KeeperPartyIds]?.ToString()
                })
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .SelectMany(s => s!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                .Distinct()
                .ToList();

            var samPartyResults = await Task.WhenAll(partyIds.Select(partyId => queryService.QueryAsync(GetSamPartiesQuery(partyId), ct)));
            var samParties = QueryResult.Combine(samPartyResults);

            var holdersQuery = GetSamCphHoldersQuery(cph);
            var holders = await queryService.QueryAsync(holdersQuery, ct);

            return new SamCphHoldingModel
            {
                Cph = cph,
                Holding = holding,
                Herd = samHerds,
                Parties = samParties,
                Holders = holders
            };
        }   
    }


    private QueryParameters GetSamHerdQuery(Cph cph) => new()
    {
        CollectionName = dataSetDefinitions.SamHerd.Name,
        Filter = BuildFilter(FilterExpression.StartsWith(DataFields.SamHerd.Cphh, cph.Value)),
        Top = 1
    };

    /// <summary>
    /// Gets the query parameters for CTS CPH Holdings.
    /// </summary>
    /// <param name="dataSetDefinitions"></param>
    /// <returns></returns>
    private QueryParameters GetCtsCphHoldingsQuery(int skip, int take) => new()
    {
        CollectionName = dataSetDefinitions.CTSCPHHolding.Name,
        Filter = BuildFilter(),
        Skip = skip,
        Top = take,
        FieldsToSelect = [CtsCphHoldingFields.LidFullIdentifier]
    };

    private QueryParameters GetCtsCphHoldingsCountQuery() => new()
    {
        CollectionName = dataSetDefinitions.CTSCPHHolding.Name,
        Filter = BuildFilter(),
        Top = 0,
        IncludeCount = true
    };

    /// <summary>
    /// Gets the CTS Keepers for a given LidFullIdentifier.
    /// </summary>
    /// <param name="dataSetDefinitions"></param>
    /// <param name="identifier"></param>
    /// <returns></returns>
    private QueryParameters GetCtsKeepersQuery(LidFullIdentifier identifier) => new()
    {
        CollectionName = dataSetDefinitions.CTSKeeper.Name,
        Filter = BuildFilter(FilterExpression.Equal(CtsKeeperFields.LidFullIdentifier, identifier.Value)),
        Top = 100,
    };

    private QueryParameters GetSamPartiesQuery(string partyId) => new()
    {
        CollectionName = dataSetDefinitions.SamParty.Name,
        Filter = BuildFilter(FilterExpression.Equal(SamPartyFields.PartyId, partyId)),
        Top = 100
    };


    private QueryParameters GetSamCphHoldingQuery(Cph cph) => new()
    {
        CollectionName = dataSetDefinitions.SamCPHHolding.Name,
        Filter = BuildFilter(FilterExpression.Equal(SamCphHoldingFields.Cph, cph.Value)),
        Top = 1
    };

    private QueryParameters GetSamCphHoldersQuery(Cph cph) => new()
    {
        CollectionName = dataSetDefinitions.SamCPHHolder.Name,
        Filter = BuildFilter(FilterExpression.Contains(SamCphHolderFields.Cphs, cph.Value)),
        Skip = 0,
        Top = 100
    };

    private QueryParameters GetSamCphHoldingsQuery(int skip, int take) => new()
    {
        CollectionName = dataSetDefinitions.SamCPHHolding.Name,
        Filter = BuildFilter(),
        Skip = skip,
        Top = take,
        FieldsToSelect = [SamCphHoldingFields.Cph]
    };

    private QueryParameters GetSamCphHoldingsCountQuery() => new()
    {
        CollectionName = dataSetDefinitions.SamCPHHolding.Name,
        Filter = BuildFilter(),
        Top = 0,
        IncludeCount = true
    };

    private QueryParameters GetCtsCphHoldingByCphQuery(Cph cph) => new()
    {
        CollectionName = dataSetDefinitions.CTSCPHHolding.Name,
        Filter = BuildFilter(FilterExpression.EndsWith(CtsCphHoldingFields.LidFullIdentifier, $"-{cph.Value}")),
        Top = 1
    };

    private static FilterExpression BuildFilter() => BuildFilter(null);

    private static FilterExpression BuildFilter(FilterExpression? filter)
    {
        if (filter == null)
        {
            return FilterExpression.Equal(IsDeleted, false);
        }
        else
        {
            return FilterExpression.And(filter, FilterExpression.Equal(IsDeleted, false));
        }
    }
}
