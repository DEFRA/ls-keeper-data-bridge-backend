using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;

public interface ICleanseAnalysisOperationsQueries
{
    Task<CleanseAnalysisOperationDto?> GetCurrentOperationAsync(CancellationToken ct = default);
    Task<CleanseAnalysisOperationDto?> GetOperationAsync(string operationId, CancellationToken ct = default);
    Task<IReadOnlyList<CleanseAnalysisOperationSummaryDto>> GetOperationsAsync(int skip, int top, CancellationToken ct = default);
}

