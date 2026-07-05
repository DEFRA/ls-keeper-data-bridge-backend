using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Worker.Configuration;

[ExcludeFromCodeCoverage(Justification = "Configuration DTO - bound from settings, no logic to test.")]
public class ScheduledJobConfiguration
{
    public string JobType { get; set; } = string.Empty;
    public bool IsEnabled { get; set; }
    public string CronSchedule { get; set; } = string.Empty;
}
