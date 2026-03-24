using FluentAssertions;
using KeeperData.Infrastructure.Benchmarking.Models;

namespace KeeperData.Infrastructure.Tests.Unit.Benchmarking.Models;

public class BenchmarkConfigTests
{
    [Fact]
    public void DefaultValues_AreReasonable()
    {
        var config = new BenchmarkConfig();

        config.SeedCount.Should().Be(10_000);
        config.Concurrency.Should().Be(4);
        config.Duration.Should().Be(TimeSpan.FromMinutes(3));
        config.ThrottleDelay.Should().Be(TimeSpan.FromMilliseconds(10));
        config.CollectionPrefix.Should().Be("_benchmark_");
    }

    [Fact]
    public void WithExpression_OverridesValues()
    {
        var config = new BenchmarkConfig
        {
            SeedCount = 500,
            Concurrency = 2,
            Duration = TimeSpan.FromSeconds(30),
            ThrottleDelay = TimeSpan.Zero,
            CollectionPrefix = "_test_"
        };

        config.SeedCount.Should().Be(500);
        config.Concurrency.Should().Be(2);
        config.Duration.Should().Be(TimeSpan.FromSeconds(30));
        config.ThrottleDelay.Should().Be(TimeSpan.Zero);
        config.CollectionPrefix.Should().Be("_test_");
    }
}
