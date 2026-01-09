using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Abstract;

/// <summary>
/// Repository for managing cleanse report items.
/// </summary>
public interface ICleanseReportRepository
{
    /// <summary>
    /// Gets a cleanse report item by its identifier.
    /// </summary>
    Task<CleanseReportItem?> GetByIdAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Inserts or updates a cleanse report item.
    /// </summary>
    Task UpsertAsync(CleanseReportItem item, CancellationToken ct = default);

    /// <summary>
    /// Activates an issue by setting is_active to true.
    /// </summary>
    Task ActivateAsync(string id, DateTime timestamp, CancellationToken ct = default);

    /// <summary>
    /// Deactivates an issue by setting is_active to false.
    /// </summary>
    Task DeactivateAsync(string id, DateTime timestamp, CancellationToken ct = default);

    /// <summary>
    /// Gets active issues with pagination.
    /// </summary>
    Task<IReadOnlyList<CleanseReportItem>> GetActiveIssuesAsync(int skip, int top, CancellationToken ct = default);
}
