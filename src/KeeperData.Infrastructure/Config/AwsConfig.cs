namespace KeeperData.Infrastructure.Config;

public class AwsConfig
{
    public const string SectionName = "AWS";

    public EmfConfig EMF { get; set; } = new();
    public MetricsConfig Metrics { get; set; } = new();
}

public class EmfConfig
{
    public string Namespace { get; set; } = "KeeperData.Bridge";
    public string ServiceName { get; set; } = "keeper-data-bridge";
}

public class MetricsConfig
{
    public string MeterName { get; set; } = "KeeperData.Bridge";
}