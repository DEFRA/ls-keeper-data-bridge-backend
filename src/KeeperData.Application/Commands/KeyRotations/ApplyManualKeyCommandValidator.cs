using FluentValidation;

namespace KeeperData.Application.Commands.KeyRotations;

public class ApplyManualKeyCommandValidator : AbstractValidator<ApplyManualKeyCommand>
{
    public ApplyManualKeyCommandValidator()
    {
        // Messages must never echo the supplied values.
        RuleFor(x => x.AccessKeyId)
            .NotEmpty()
            .WithMessage("AccessKeyId is required.");

        RuleFor(x => x.SecretAccessKey)
            .NotEmpty()
            .WithMessage("SecretAccessKey is required.");
    }
}
