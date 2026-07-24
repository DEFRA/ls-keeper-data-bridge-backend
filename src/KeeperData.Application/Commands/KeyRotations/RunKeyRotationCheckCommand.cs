using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Application.Commands.KeyRotations;

/// <summary>
/// Runs the external storage key rotation check on demand — the same detect/validate/adopt
/// flow the scheduled 3am job performs.
/// </summary>
public record RunKeyRotationCheckCommand : ICommand<KeyRotationCheckResponse>;

/// <summary>
/// The outcome of an on-demand rotation check, safe for API exposure: only masked key
/// material is included.
/// </summary>
public record KeyRotationCheckResponse
{
    public required KeyRotationCheckOutcome Outcome { get; init; }
    public required string BucketName { get; init; }
    public string? FileKey { get; init; }
    public string? FileHash { get; init; }
    public string? KeyIdHint { get; init; }
    public string? Detail { get; init; }
    public DateTime CheckedAtUtc { get; init; }

    public static KeyRotationCheckResponse FromResult(KeyRotationCheckResult result, DateTime checkedAtUtc) => new()
    {
        Outcome = result.Outcome,
        BucketName = result.BucketName,
        FileKey = result.FileKey,
        FileHash = result.FileHash,
        KeyIdHint = result.KeyIdHint,
        Detail = result.Detail,
        CheckedAtUtc = checkedAtUtc
    };
}
