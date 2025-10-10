using KeeperData.Infrastructure.Config;
using KeeperData.Infrastructure.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace KeeperData.Infrastructure.Tests.Unit.Metrics;

public class EmfMetricsServiceTests
{
    [Fact]
    public void PutMetric_WithDimensions_ShouldNotThrow()
    {
        // Arrange
        var logger = new Mock<ILogger<EmfMetricsService>>();
        var loggerFactory = new Mock<ILoggerFactory>();
        var awsConfig = Options.Create(new AwsConfig
        {
            EMF = new EmfConfig
            {
                Namespace = "TestNamespace",
                ServiceName = "TestService"
            }
        });

        var service = new EmfMetricsService(logger.Object, awsConfig, loggerFactory.Object);
        var dimensions = new Dictionary<string, string>
        {
            { "Environment", "Test" },
            { "Operation", "TestOperation" }
        };

        // Act & Assert - Should not throw
        var exception = Record.Exception(() => 
            service.PutMetric("TestMetric", 42.0, "Count", dimensions));
        
        Assert.Null(exception);
    }

    [Fact]
    public void PutMetric_WithoutDimensions_ShouldNotThrow()
    {
        // Arrange
        var logger = new Mock<ILogger<EmfMetricsService>>();
        var loggerFactory = new Mock<ILoggerFactory>();
        var awsConfig = Options.Create(new AwsConfig
        {
            EMF = new EmfConfig
            {
                Namespace = "TestNamespace", 
                ServiceName = "TestService"
            }
        });

        var service = new EmfMetricsService(logger.Object, awsConfig, loggerFactory.Object);

        // Act & Assert - Should not throw
        var exception = Record.Exception(() => 
            service.PutMetric("TestMetric", 1.0, (Dictionary<string, string>?)null));
        
        Assert.Null(exception);
    }
}