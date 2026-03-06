using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling.Models;

[ExcludeFromCodeCoverage]
public sealed record ThrottlePolicy
{
    public required string Slug { get; init; }
    public required string Name { get; init; }
    public bool IsActive { get; init; }
    public bool IsReadOnly { get; init; }
    public required ThrottlePolicySettings Settings { get; init; }
    public DateTime CreatedAtUtc { get; init; } = DateTime.UtcNow;
    public DateTime UpdatedAtUtc { get; init; } = DateTime.UtcNow;
}
