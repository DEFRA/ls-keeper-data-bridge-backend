using KeeperData.Core.Domain.Entities;

namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// Persistence for <see cref="KeyRotationRecord"/> history (append-only).
/// </summary>
public interface IKeyRotationRepository
{
    /// <summary>Gets the currently active rotation record, or null when none exists.</summary>
    Task<KeyRotationRecord?> GetActiveAsync(CancellationToken cancellationToken = default);

    /// <summary>Gets a rotation record by id, or null when not found.</summary>
    Task<KeyRotationRecord?> GetByIdAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the file hash of the most recently observed rotation file (successful or failed),
    /// or null when no file has ever been processed.
    /// </summary>
    Task<string?> GetLatestObservedFileHashAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Activates <paramref name="record"/>: supersedes the current active record (if any)
    /// and inserts the new record as active.
    /// </summary>
    Task ActivateAsync(KeyRotationRecord record, CancellationToken cancellationToken = default);

    /// <summary>Appends a failed rotation record (does not touch the active record).</summary>
    Task AddFailedAsync(KeyRotationRecord record, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a page of successful rotations (active and superseded), most recent first.
    /// </summary>
    Task<KeyRotationPage> GetSuccessfulPageAsync(int page, int pageSize, CancellationToken cancellationToken = default);
}

/// <summary>A page of rotation records plus the total count of successful rotations.</summary>
public sealed record KeyRotationPage(IReadOnlyList<KeyRotationRecord> Items, long TotalCount);
