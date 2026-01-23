using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Records and resolves cleanse issues using the report repository.
/// </summary>
public sealed class IssueRecorder : IIssueRecorder
{
    private readonly ICleanseReportRepository _repository;

    public IssueRecorder(ICleanseReportRepository repository)
    {
        _repository = repository;
    }

    /// <inheritdoc />
    public async Task<IssueRecordResult> RecordIssueAsync(
        string thumbprint,
        string issueCode,
        LidFullIdentifier lidFullIdentifier,
        CancellationToken ct)
    {
        var existing = await _repository.GetByIdAsync(thumbprint, ct);
        var now = DateTime.UtcNow;

        if (existing is null)
        {
            var item = new CleanseReportItem
            {
                Id = thumbprint,
                Code = issueCode,
                CtsLidFullIdentifier = lidFullIdentifier.Value,
                Cph = lidFullIdentifier.Cph.Value,
                CreatedAtUtc = now,
                LastUpdatedAtUtc = now,
                IsActive = true
            };
            await _repository.UpsertAsync(item, ct);
            return IssueRecordResult.Created;
        }

        if (!existing.IsActive)
        {
            await _repository.ActivateAsync(thumbprint, now, ct);
            return IssueRecordResult.Reactivated;
        }

        return IssueRecordResult.NoChange;
    }

    /// <inheritdoc />
    public async Task<IssueRecordResult> ResolveIssueIfExistsAsync(string thumbprint, CancellationToken ct)
    {
        var existing = await _repository.GetByIdAsync(thumbprint, ct);

        if (existing?.IsActive == true)
        {
            await _repository.DeactivateAsync(thumbprint, DateTime.UtcNow, ct);
            return IssueRecordResult.Resolved;
        }

        return IssueRecordResult.NoChange;
    }
}
