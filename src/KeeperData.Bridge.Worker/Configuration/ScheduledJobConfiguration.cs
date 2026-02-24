namespace KeeperData.Bridge.Worker.Configuration;

public class ScheduledJobConfiguration
{
    public string JobType { get; set; } = string.Empty;
    public bool IsEnabled { get; set; }
    public string CronSchedule { get; set; } = string.Empty;
}