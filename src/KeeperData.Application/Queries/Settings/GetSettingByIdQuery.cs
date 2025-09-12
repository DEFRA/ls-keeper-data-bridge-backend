using KeeperData.Core.Domain.Entities;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Application.Queries.Settings;

/// <summary>
/// Example usage only. Delete in future changes.
/// </summary>
/// <param name="Id"></param>
[ExcludeFromCodeCoverage]
public record GetSettingByIdQuery(string Id) : IQuery<Setting>;