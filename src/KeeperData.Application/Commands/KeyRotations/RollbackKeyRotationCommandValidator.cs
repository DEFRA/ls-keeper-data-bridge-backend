using FluentValidation;

namespace KeeperData.Application.Commands.KeyRotations;

public class RollbackKeyRotationCommandValidator : AbstractValidator<RollbackKeyRotationCommand>
{
    public RollbackKeyRotationCommandValidator()
    {
        RuleFor(x => x.RotationId)
            .NotEmpty()
            .WithMessage("RotationId is required.");
    }
}
