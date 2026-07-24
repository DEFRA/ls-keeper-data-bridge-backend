using FluentAssertions;
using FluentValidation.TestHelper;
using KeeperData.Application.Queries.KeyRotations;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;
using Moq;

namespace KeeperData.Application.Tests.Unit.Queries.KeyRotations;

public class GetKeyRotationsQueryHandlerTests
{
    private readonly Mock<IKeyRotationRepository> _repositoryMock = new();
    private readonly GetKeyRotationsQueryHandler _sut;

    public GetKeyRotationsQueryHandlerTests()
    {
        _sut = new GetKeyRotationsQueryHandler(_repositoryMock.Object);
    }

    [Fact]
    public async Task Handle_MapsRecordsToMaskedListItems()
    {
        // Arrange
        var record = new KeyRotationRecord
        {
            Id = "rotation-1",
            BucketName = "cerespfm-prd-prd1-livestockfeeds",
            RotatedAtUtc = new DateTime(2026, 7, 16, 3, 0, 12, DateTimeKind.Utc),
            Source = KeyRotationSource.Automatic,
            Status = KeyRotationStatus.Active,
            FileKey = "Prd1_LI_CDP_Int_User_accessKeys.csv",
            FileHash = "abc123",
            KeyIdMasked = "AKI...DEF",
            EncryptedAccessKeyId = new EncryptedSecret { KeyVersion = 1, Nonce = "n", CipherText = "c", Tag = "t" },
            EncryptedSecretAccessKey = new EncryptedSecret { KeyVersion = 1, Nonce = "n", CipherText = "c", Tag = "t" }
        };
        _repositoryMock.Setup(r => r.GetSuccessfulPageAsync(1, 10, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KeyRotationPage([record], 1));

        // Act
        var result = await _sut.Handle(new GetKeyRotationsQuery(), CancellationToken.None);

        // Assert
        result.TotalCount.Should().Be(1);
        result.Page.Should().Be(1);
        result.PageSize.Should().Be(10);

        var item = result.Items.Single();
        item.Id.Should().Be("rotation-1");
        item.RotatedAtUtc.Should().Be(record.RotatedAtUtc);
        item.FileHash.Should().Be("abc123");
        item.KeyIdHint.Should().Be("AKI...DEF");
        item.Source.Should().Be("Automatic");
        item.Status.Should().Be("Active");
    }

    [Fact]
    public async Task Handle_PassesPagingThroughToRepository()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetSuccessfulPageAsync(3, 25, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KeyRotationPage([], 60));

        // Act
        var result = await _sut.Handle(new GetKeyRotationsQuery(3, 25), CancellationToken.None);

        // Assert
        result.Page.Should().Be(3);
        result.PageSize.Should().Be(25);
        result.TotalCount.Should().Be(60);
        _repositoryMock.Verify(r => r.GetSuccessfulPageAsync(3, 25, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public void ListItem_NeverExposesEncryptedMaterial()
    {
        // Assert - compile-time shape check: the DTO has no key/secret fields.
        typeof(KeyRotationListItem).GetProperties()
            .Select(p => p.Name)
            .Should().BeEquivalentTo(
                "Id", "RotatedAtUtc", "FileKey", "FileHash", "KeyIdHint", "Source", "Status", "RolledBackFromId");
    }
}

public class GetKeyRotationsQueryValidatorTests
{
    private readonly GetKeyRotationsQueryValidator _validator = new();

    [Fact]
    public void Validate_WithDefaults_Passes()
    {
        // Act
        var result = _validator.TestValidate(new GetKeyRotationsQuery());

        // Assert
        result.ShouldNotHaveAnyValidationErrors();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_WithInvalidPage_Fails(int page)
    {
        // Act
        var result = _validator.TestValidate(new GetKeyRotationsQuery(page));

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Page);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(101)]
    public void Validate_WithInvalidPageSize_Fails(int pageSize)
    {
        // Act
        var result = _validator.TestValidate(new GetKeyRotationsQuery(1, pageSize));

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.PageSize);
    }
}
