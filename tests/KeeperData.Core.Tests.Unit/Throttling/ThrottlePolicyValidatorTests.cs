using FluentAssertions;
using KeeperData.Core.Throttling.Models;
using KeeperData.Core.Throttling.Validation;

namespace KeeperData.Core.Tests.Unit.Throttling;

public class ThrottlePolicyValidatorTests
{
    [Fact]
    public void Validate_WithDefaults_ShouldReturnNoErrors()
    {
        var errors = ThrottlePolicyValidator.Validate("Test", new ThrottlePolicySettings());
        errors.Should().BeEmpty();
    }

    [Fact]
    public void Validate_WithEmptyName_ShouldReturnError()
    {
        var errors = ThrottlePolicyValidator.Validate("", new ThrottlePolicySettings());
        errors.Should().Contain(e => e.Contains("Name"));
    }

    [Fact]
    public void Validate_WithLongName_ShouldReturnError()
    {
        var name = new string('x', 101);
        var errors = ThrottlePolicyValidator.Validate(name, new ThrottlePolicySettings());
        errors.Should().Contain(e => e.Contains("100"));
    }

    [Fact]
    public void Validate_WithZeroBatchSize_ShouldReturnError()
    {
        var settings = new ThrottlePolicySettings
        {
            Ingestion = new() { BatchSize = 0 }
        };

        var errors = ThrottlePolicyValidator.Validate("Test", settings);
        errors.Should().Contain(e => e.Contains("Ingestion.BatchSize"));
    }

    [Fact]
    public void Validate_WithNegativeDelay_ShouldReturnError()
    {
        var settings = new ThrottlePolicySettings
        {
            Ingestion = new() { BatchDelayMs = -1 }
        };

        var errors = ThrottlePolicyValidator.Validate("Test", settings);
        errors.Should().Contain(e => e.Contains("Ingestion.BatchDelayMs"));
    }

    [Fact]
    public void Validate_WithExcessiveDelay_ShouldReturnError()
    {
        var settings = new ThrottlePolicySettings
        {
            CleanseAnalysis = new() { PumpDelayMs = 70000 }
        };

        var errors = ThrottlePolicyValidator.Validate("Test", settings);
        errors.Should().Contain(e => e.Contains("CleanseAnalysis.PumpDelayMs"));
    }

    [Fact]
    public void Validate_WithBoundaryValues_ShouldPass()
    {
        var settings = new ThrottlePolicySettings
        {
            Ingestion = new() { BatchSize = 1, BatchDelayMs = 0 },
            CleanseAnalysis = new() { PumpBatchSize = 5000, PumpDelayMs = 60000 }
        };

        var errors = ThrottlePolicyValidator.Validate("Test", settings);
        errors.Should().BeEmpty();
    }
}
