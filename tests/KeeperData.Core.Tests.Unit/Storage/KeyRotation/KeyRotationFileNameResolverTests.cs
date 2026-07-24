using FluentAssertions;
using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Core.Tests.Unit.Storage.KeyRotation;

public class KeyRotationFileNameResolverTests
{
    [Theory]
    [InlineData("cerespfm-prd-prd1-livestockfeeds", "Prd1_LI_CDP_Int_User_accessKeys.csv")]
    [InlineData("cerespfm-dev-dev1-livestockfeeds", "Dev1_LI_CDP_Int_User_accessKeys.csv")]
    [InlineData("cerespfm-dev-sys1-livestockfeeds", "Sys1_LI_CDP_Int_User_accessKeys.csv")]
    [InlineData("cerespfm-prp-prp1-livestockfeeds", "Prp1_LI_CDP_Int_User_accessKeys.csv")]
    public void Resolve_WithRealBucketNames_ReturnsExpectedFileName(string bucketName, string expected)
    {
        // Act
        var result = KeyRotationFileNameResolver.Resolve(bucketName);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void Resolve_WithThirdSegmentAlreadyCapitalised_PreservesRemainder()
    {
        // Act
        var result = KeyRotationFileNameResolver.Resolve("a-b-QA2x-d");

        // Assert
        result.Should().Be("QA2x_LI_CDP_Int_User_accessKeys.csv");
    }

    [Theory]
    [InlineData("no-hyphens")]
    [InlineData("only--")]
    [InlineData("a-b")]
    public void Resolve_WithoutUsableThirdSegment_Throws(string bucketName)
    {
        // Act
        var act = () => KeyRotationFileNameResolver.Resolve(bucketName);

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*third hyphen-separated segment*");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Resolve_WithMissingBucketName_Throws(string? bucketName)
    {
        // Act
        var act = () => KeyRotationFileNameResolver.Resolve(bucketName!);

        // Assert
        act.Should().Throw<ArgumentException>();
    }
}
