using FluentAssertions;
using KeeperData.Core.Database.Configuration;

namespace KeeperData.Core.Tests.Unit.Database.Configuration;

public class MongoResilienceConfigTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var config = new MongoResilienceConfig();

        config.MaxRetryAttempts.Should().Be(3);
        config.InitialDelayMs.Should().Be(500);
        config.TimeoutSeconds.Should().Be(30);
        config.EnableCircuitBreaker.Should().BeTrue();
        config.CircuitBreakerFailureRatio.Should().Be(0.5);
        config.CircuitBreakerMinimumThroughput.Should().Be(10);
        config.CircuitBreakerBreakDurationSeconds.Should().Be(30);
    }

    [Fact]
    public void CustomValues_CanBeSet()
    {
        var config = new MongoResilienceConfig
        {
            MaxRetryAttempts = 5,
            InitialDelayMs = 1000,
            TimeoutSeconds = 60,
            EnableCircuitBreaker = false,
            CircuitBreakerFailureRatio = 0.8,
            CircuitBreakerMinimumThroughput = 20,
            CircuitBreakerBreakDurationSeconds = 60
        };

        config.MaxRetryAttempts.Should().Be(5);
        config.InitialDelayMs.Should().Be(1000);
        config.TimeoutSeconds.Should().Be(60);
        config.EnableCircuitBreaker.Should().BeFalse();
        config.CircuitBreakerFailureRatio.Should().Be(0.8);
        config.CircuitBreakerMinimumThroughput.Should().Be(20);
        config.CircuitBreakerBreakDurationSeconds.Should().Be(60);
    }

    [Fact]
    public void Record_SupportsWithExpression()
    {
        var original = new MongoResilienceConfig();
        var modified = original with { MaxRetryAttempts = 10, TimeoutSeconds = 120 };

        modified.MaxRetryAttempts.Should().Be(10);
        modified.TimeoutSeconds.Should().Be(120);
        // Other values should remain defaults
        modified.InitialDelayMs.Should().Be(500);
        modified.EnableCircuitBreaker.Should().BeTrue();
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var config1 = new MongoResilienceConfig { MaxRetryAttempts = 5 };
        var config2 = new MongoResilienceConfig { MaxRetryAttempts = 5 };
        var config3 = new MongoResilienceConfig { MaxRetryAttempts = 3 };

        config1.Should().Be(config2);
        config1.Should().NotBe(config3);
    }
}
