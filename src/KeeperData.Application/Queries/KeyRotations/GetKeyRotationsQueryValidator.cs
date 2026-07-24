using FluentValidation;

namespace KeeperData.Application.Queries.KeyRotations;

public class GetKeyRotationsQueryValidator : AbstractValidator<GetKeyRotationsQuery>
{
    public const int MaxPageSize = 100;

    public GetKeyRotationsQueryValidator()
    {
        RuleFor(x => x.Page)
            .GreaterThanOrEqualTo(1)
            .WithMessage("Page must be greater than or equal to 1.");

        RuleFor(x => x.PageSize)
            .InclusiveBetween(1, MaxPageSize)
            .WithMessage($"PageSize must be between 1 and {MaxPageSize}.");
    }
}
