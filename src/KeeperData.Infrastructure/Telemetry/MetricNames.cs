namespace KeeperData.Infrastructure.Telemetry;

public static class MetricNames
{
    public const string MeterName = "KeeperData";

    public static class CommonTags
    {
        public const string Service = "keeperdata.service";
        public const string HealthCheck = "keeperdata.healthcheck";
        public const string Status = "keeperdata.status";
    }
}