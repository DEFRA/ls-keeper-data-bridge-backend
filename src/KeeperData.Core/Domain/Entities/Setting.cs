using KeeperData.Core.Attributes;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Domain.Entities;

/// <summary>
/// Example usage only. Delete in future changes.
/// </summary>
[ExcludeFromCodeCoverage]
[CollectionName("settings")]
public class Setting : IEntity
{
    public required string Id { get; set; }
    public string? Name { get; set; }
}