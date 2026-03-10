using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries;

[ExcludeFromCodeCoverage(Justification = "MongoDB query class - covered by integration tests.")]
public class CleanseAnalysisOperationsQueries(
    CleanseOperationsCollection operationsCollection,
    ICleanseRunStatsService runStatsService)
    : ICleanseAnalysisOperationsQueries
{
    private readonly IMongoCollection<CleanseAnalysisOperationDocument> _collection = operationsCollection.Collection;

    public async Task<CleanseAnalysisOperationDto?> GetOperationAsync(string operationId, CancellationToken ct = default)
    {
        var filter = Builders<CleanseAnalysisOperationDocument>.Filter.Eq(d => d.Id, operationId);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        var dto = document?.ToDto();
        EnrichWithStats(dto);
        return dto;
    }

    public async Task<IReadOnlyList<CleanseAnalysisOperationSummaryDto>> GetOperationsAsync(int skip, int top, CancellationToken ct = default)
    {
        var documents = await _collection
            .Find(Builders<CleanseAnalysisOperationDocument>.Filter.Empty)
            .SortByDescending(d => d.StartedAtUtc)
            .Skip(skip)
            .Limit(top)
            .ToListAsync(ct);
        var summaries = documents.Select(d => d.ToSummaryDto()).ToList();
        foreach (var summary in summaries)
        {
            EnrichSummaryWithStats(summary);
        }
        return summaries;
    }

    public async Task<CleanseAnalysisOperationDto?> GetCurrentOperationAsync(CancellationToken ct = default)
    {
        var filter = Builders<CleanseAnalysisOperationDocument>.Filter.In(
            d => d.Status,
            new[] { CleanseAnalysisStatus.Running.ToString(), CleanseAnalysisStatus.Cancelling.ToString() });
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        var dto = document?.ToDto();
        EnrichWithStats(dto);
        return dto;
    }

    private void EnrichWithStats(CleanseAnalysisOperationDto? dto)
    {
        if (dto is null || (dto.Status != CleanseAnalysisStatus.Running && dto.Status != CleanseAnalysisStatus.Cancelling))
            return;

        dto.Stats = runStatsService.CalculateStats(dto.Id, dto.RecordsAnalyzed, dto.TotalRecords, dto.StartedAtUtc);
    }

    private void EnrichSummaryWithStats(CleanseAnalysisOperationSummaryDto summary)
    {
        if (summary.Status != CleanseAnalysisStatus.Running.ToString() &&
            summary.Status != CleanseAnalysisStatus.Cancelling.ToString())
            return;

        summary.Stats = runStatsService.CalculateStats(summary.Id, summary.RecordsAnalyzed, summary.TotalRecords, summary.StartedAtUtc);
    }
}

