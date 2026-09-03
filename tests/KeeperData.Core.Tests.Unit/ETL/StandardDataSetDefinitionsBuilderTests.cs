using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using Xunit;

namespace KeeperData.Core.Tests.Unit.ETL;

[Trait("Category", "Unit")]
public class StandardDataSetDefinitionsBuilderTests
{
    [Fact]
    public void Build_WithoutAFolder_FoldersEveryDefinitionUnderLitprd()
    {
        // Act
        var definitions = StandardDataSetDefinitionsBuilder.Build();

        // Assert
        definitions.All.Should().OnlyContain(d => d.FilePrefixFormat.StartsWith("litprd/"));
        definitions.SamCPHHolding.FilePrefixFormat.Should().Be("litprd/LITP_SAMCPHHOLDING_{0}");
    }

    [Theory]
    [InlineData("litprd")]
    [InlineData("litprd/")]
    [InlineData("/litprd/")]
    [InlineData("  litprd  ")]
    public void Build_WithAConfiguredFolder_YieldsASinglySlashedPrefix(string configured)
    {
        // Act
        var definitions = StandardDataSetDefinitionsBuilder.Build(configured);

        // Assert
        definitions.SamCPHHolding.FilePrefixFormat.Should().Be("litprd/LITP_SAMCPHHOLDING_{0}");
    }

    [Fact]
    public void Build_WithAnotherFolder_MovesEveryDefinition()
    {
        // Act
        var definitions = StandardDataSetDefinitionsBuilder.Build("feeds/litprd");

        // Assert
        definitions.All.Should().OnlyContain(d => d.FilePrefixFormat.StartsWith("feeds/litprd/"));
        definitions.SamShowground.FilePrefixFormat.Should().Be("feeds/litprd/LITP_SAMSHOWGROUND_{0}");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("/")]
    public void Build_WithNoFolder_LeavesTheFilesAtTheBucketRoot(string configured)
    {
        // Act
        var definitions = StandardDataSetDefinitionsBuilder.Build(configured);

        // Assert
        definitions.SamCPHHolding.FilePrefixFormat.Should().Be("LITP_SAMCPHHOLDING_{0}");
    }

    [Fact]
    public void Build_FormatsATimestampIntoTheFolderedPrefix()
    {
        // Arrange
        var definition = StandardDataSetDefinitionsBuilder.Build().SamCPHHolding;

        // Act
        var prefix = string.Format(definition.FilePrefixFormat, "20260822120000");

        // Assert
        prefix.Should().Be("litprd/LITP_SAMCPHHOLDING_20260822120000");
    }
}
