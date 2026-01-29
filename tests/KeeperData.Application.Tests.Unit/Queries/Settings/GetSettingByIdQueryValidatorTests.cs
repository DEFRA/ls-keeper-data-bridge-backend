using FluentAssertions;
using FluentValidation.TestHelper;
using KeeperData.Application.Queries.Settings;

namespace KeeperData.Application.Tests.Unit.Queries.Settings;

public class GetSettingByIdQueryValidatorTests
{
    private readonly GetSettingByIdQueryValidator _validator = new();

    [Fact]
    public void Validate_WithValidId_ShouldNotHaveErrors()
    {
        // Arrange
        var query = new GetSettingByIdQuery("valid-id-123");

        // Act
        var result = _validator.TestValidate(query);

        // Assert
        result.ShouldNotHaveAnyValidationErrors();
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void Validate_WithEmptyId_ShouldHaveError(string? id)
    {
        // Arrange
        var query = new GetSettingByIdQuery(id!);

        // Act
        var result = _validator.TestValidate(query);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Id);
    }
}
