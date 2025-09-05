using Elastic.CommonSchema.Serilog;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Telemetry.Logging;

[ExcludeFromCodeCoverage]
public static class SerilogLoggingExtensions
{
    public static void AddLogging(HostBuilderContext ctx, LoggerConfiguration config)
    {
        var httpAccessor = ctx.Configuration.Get<HttpContextAccessor>();
        var traceIdHeader = ctx.Configuration.GetValue<string>("TraceHeader");
        var serviceVersion = Environment.GetEnvironmentVariable("SERVICE_VERSION") ?? "";

        config
            .ReadFrom.Configuration(ctx.Configuration)
            .Enrich.WithEcsHttpContext(httpAccessor!)
            .Enrich.FromLogContext()
            .Enrich.WithProperty("service.version", serviceVersion);

        if (traceIdHeader != null)
        {
            config.Enrich.WithCorrelationId(traceIdHeader);
        }
    }
}