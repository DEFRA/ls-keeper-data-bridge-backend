using FluentValidation;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Application.Commands.Settings;

/// <summary>
/// Example usage only. Delete in future changes.
/// </summary>
[ExcludeFromCodeCoverage]
public class CreateSettingCommandValidator : AbstractValidator<CreateSettingCommand>
{
    public CreateSettingCommandValidator()
    {
        RuleFor(x => x.Name)
            .NotEmpty()
            .MaximumLength(100);
    }
}