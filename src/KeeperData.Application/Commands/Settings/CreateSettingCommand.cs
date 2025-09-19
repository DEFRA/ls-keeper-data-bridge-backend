using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Application.Commands.Settings;

/// <summary>
/// Example usage only. Delete in future changes.
/// </summary>
/// <param name="Name"></param>
[ExcludeFromCodeCoverage]
public record CreateSettingCommand(string Name) : ICommand<string>;