using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Commands;

[ExcludeFromCodeCoverage]
public sealed record UpdateThrottlePolicyCommand
{
    public string? Name { get; init; }
    public required ThrottlePolicySettings Settings { get; init; }
}
