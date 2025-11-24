using KeeperData.Bridge.Setup;
using KeeperData.Bridge.Utils;
using KeeperData.Infrastructure.Telemetry.Logging;
using Serilog;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

var app = CreateWebApplication(args);
await app.RunAsync();
return;

[ExcludeFromCodeCoverage]
static WebApplication CreateWebApplication(string[] args)
{
    var builder = WebApplication.CreateBuilder(args);
    ConfigureBuilder(builder);

    var app = builder.Build();

    app.ConfigureRequestPipeline();

    return app;
}

[ExcludeFromCodeCoverage]
static void ConfigureBuilder(WebApplicationBuilder builder)
{
    builder.Configuration.AddEnvironmentVariables();

    // Load certificates into Trust Store - Note must happen before Mongo and Http client connections.
    builder.Services.AddCustomTrustStore();

    // Configure logging to use the CDP Platform standards.
    builder.Services.AddHttpContextAccessor();
    builder.Host.UseSerilog(SerilogLoggingExtensions.AddLogging);

    // Default HTTP Client
    builder.Services
        .AddHttpClient("DefaultClient")
        .AddHeaderPropagation();

    // Proxy HTTP Client
    builder.Services.AddTransient<ProxyHttpMessageHandler>();
    builder.Services
        .AddHttpClient("proxy")
        .ConfigurePrimaryHttpMessageHandler<ProxyHttpMessageHandler>();

    // Propagate trace header.
    builder.Services.AddHeaderPropagation(options =>
    {
        var traceHeader = builder.Configuration.GetValue<string>("TraceHeader");
        if (!string.IsNullOrWhiteSpace(traceHeader))
        {
            options.Headers.Add(traceHeader);
        }
    });

    // Configure culture to en-GB
    var supportedCultures = new[] { new CultureInfo("en-GB") };
    builder.Services.Configure<RequestLocalizationOptions>(options =>
    {
        options.DefaultRequestCulture = new Microsoft.AspNetCore.Localization.RequestCulture("en-GB");
        options.SupportedCultures = supportedCultures;
        options.SupportedUICultures = supportedCultures;
    });

    builder.Services.ConfigureApi(builder.Configuration);
}

public partial class Program { }