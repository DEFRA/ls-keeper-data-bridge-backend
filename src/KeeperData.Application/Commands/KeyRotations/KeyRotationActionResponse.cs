using KeeperData.Application.Queries.KeyRotations;
using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Application.Commands.KeyRotations;

/// <summary>
/// Result of a manual apply or rollback, safe for API exposure: the record summary is
/// masked and never contains key material.
/// </summary>
public record KeyRotationActionResponse
{
    public required KeyRotationActionOutcome Outcome { get; init; }
    public KeyRotationListItem? Rotation { get; init; }
    public string? Detail { get; init; }

    public static KeyRotationActionResponse FromResult(KeyRotationActionResult result) => new()
    {
        Outcome = result.Outcome,
        Rotation = result.Record is null ? null : KeyRotationListItem.FromRecord(result.Record),
        Detail = result.Detail
    };
}
