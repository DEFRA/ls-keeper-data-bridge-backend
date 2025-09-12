using FluentAssertions;
using FluentValidation;
using KeeperData.Infrastructure.Behaviors;
using MediatR;

namespace KeeperData.Infrastructure.Tests.Unit.Behaviors;

public class ValidationBehaviorTests
{
    [Fact]
    public async Task GivenValidRequest_WhenHandled_ThenCallsNextAndReturnsResponse()
    {
        var request = new ValidationTestRequest { Name = "Valid Name" };
        var validators = new List<IValidator<ValidationTestRequest>> { new ValidationTestRequestValidator() };
        var behavior = new ValidationBehavior<ValidationTestRequest, string>(validators);

        var expectedResponse = "Success";
        Task<string> next(CancellationToken token = default) => Task.FromResult(expectedResponse);

        var result = await behavior.Handle(request, next, CancellationToken.None);

        result.Should().Be(expectedResponse);
    }

    [Fact]
    public async Task GivenInvalidRequest_WhenHandled_ThenThrowsValidationException()
    {
        var request = new ValidationTestRequest { Name = "" };
        var validators = new List<IValidator<ValidationTestRequest>> { new ValidationTestRequestValidator() };
        var behavior = new ValidationBehavior<ValidationTestRequest, string>(validators);

        static Task<string> next(CancellationToken token = default) => Task.FromResult("Should not reach");

        var exception = await Assert.ThrowsAsync<ValidationException>(() =>
            behavior.Handle(request, next, CancellationToken.None));

        exception.Errors.Should().ContainSingle()
            .Which.ErrorMessage.Should().Be("Name is required.");
    }

    [Fact]
    public async Task GivenNoValidators_WhenHandled_ThenCallsNext()
    {
        var request = new ValidationTestRequest { Name = "" };
        var behavior = new ValidationBehavior<ValidationTestRequest, string>([]);

        var expectedResponse = "No validators";
        Task<string> next(CancellationToken token = default) => Task.FromResult(expectedResponse);

        var result = await behavior.Handle(request, next, CancellationToken.None);

        result.Should().Be(expectedResponse);
    }
}

public class ValidationTestRequest : IRequest<string>
{
    public string Name { get; set; } = string.Empty;
}

public class ValidationTestRequestValidator : AbstractValidator<ValidationTestRequest>
{
    public ValidationTestRequestValidator()
    {
        RuleFor(x => x.Name).NotEmpty().WithMessage("Name is required.");
    }
}