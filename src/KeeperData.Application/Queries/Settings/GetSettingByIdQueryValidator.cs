using FluentValidation;

namespace KeeperData.Application.Queries.Settings;

/// <summary>
/// Example usage only. Delete in future changes.
/// </summary>
public class GetSettingByIdQueryValidator : AbstractValidator<GetSettingByIdQuery>
{
    public GetSettingByIdQueryValidator()
    {
        RuleFor(x => x.Id).NotEmpty();
    }
}