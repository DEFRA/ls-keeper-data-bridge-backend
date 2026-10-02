using KeeperData.Etl.Tool.Configuration;
using KeeperData.Etl.Tool.Setup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.CommandLine;

namespace KeeperData.Etl.Tool;

/// <summary>Builds the host once the command line has been parsed, so a CLI option can override a
/// setting rather than only supplement it.</summary>
internal static class EtlToolHost
{
    /// <summary>The environment variable the salt is kept in. PasswordSaltService reads the plain
    /// AesSalt key, so the value is copied across rather than the service being taught a second name.</summary>
    private const string SaltEnvironmentVariable = "S3_AESSALT";

    public static IHost Build(ParseResult parseResult, bool quiet = false)
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Configuration.AddEnvironmentVariables();

        // Two passes: the first settles the tool's own options, the second derives the storage
        // settings from them - which bucket, which credential variables, where the folders live.
        builder.Configuration.AddInMemoryCollection(ToolOverrides(parseResult, builder.Configuration));

        var options = builder.Configuration.GetSection(EtlToolOptions.SectionName).Get<EtlToolOptions>()
                      ?? new EtlToolOptions();

        builder.Configuration.AddInMemoryCollection(StorageOverrides(options));

        builder.Logging.ClearProviders();

        if (!quiet)
        {
            builder.Logging.AddSimpleConsole(logging =>
            {
                logging.SingleLine = true;
                logging.TimestampFormat = "HH:mm:ss ";
            });

            builder.Logging.SetMinimumLevel(LogLevel.Information);

            // The executor's own stage logging would double every line the progress observer writes.
            builder.Logging.AddFilter("KeeperData.Core.Pipeline.PipelineExecutor", LogLevel.Warning);
        }

        builder.Services.AddEtlTool(builder.Configuration);

        return builder.Build();
    }

    private static Dictionary<string, string?> ToolOverrides(ParseResult parseResult, IConfiguration configuration)
    {
        var overrides = new Dictionary<string, string?>();

        Set(Commands.CommonOptions.Environment, nameof(EtlToolOptions.Environment));
        Set(Commands.CommonOptions.Staging, nameof(EtlToolOptions.StagingPath));

        if (string.IsNullOrWhiteSpace(configuration["AesSalt"])
            && System.Environment.GetEnvironmentVariable(SaltEnvironmentVariable) is { Length: > 0 } salt)
        {
            overrides["AesSalt"] = salt;
        }

        return overrides;

        void Set(Option<string?> option, string property)
        {
            var value = parseResult.GetValue(option);

            if (!string.IsNullOrWhiteSpace(value))
            {
                overrides[$"{EtlToolOptions.SectionName}:{property}"] = value;
            }
        }
    }

    /// <summary>The settings AddStorageDependencies reads. UseFileSystem keeps every ETL folder on
    /// disk while the supplier's bucket stays remote, which is what the production file-system
    /// factory already does - its GetSourceExternal is the one container it leaves on S3.</summary>
    private static Dictionary<string, string?> StorageOverrides(EtlToolOptions options)
        => new()
        {
            ["StorageConfiguration:ExternalStorage:AccessKeySecretName"] = options.AccessKeyVariable,
            ["StorageConfiguration:ExternalStorage:SecretKeySecretName"] = options.SecretKeyVariable,
            ["StorageConfiguration:ExternalStorage:BucketName"] = BucketName(options.BucketVariable),
            ["StorageConfiguration:ExternalStorage:ServiceUrl"] = System.Environment.GetEnvironmentVariable(options.EndpointVariable),
            ["StorageConfiguration:ExternalStorage:HealthcheckEnabled"] = "false",
            ["StorageConfiguration:InternalStorage:HealthcheckEnabled"] = "false",
            ["StorageConfiguration:UseFileSystem"] = "true",
            ["StorageConfiguration:FileSystemBasePath"] = options.StagingPath
        };

    /// <summary>The bucket variables hold a URI - s3://name/ - but the SDK wants the bare name.</summary>
    public static string? BucketName(string variable)
    {
        var value = System.Environment.GetEnvironmentVariable(variable);

        if (string.IsNullOrWhiteSpace(value)) return value;

        return value.Replace("s3://", string.Empty, StringComparison.OrdinalIgnoreCase).Trim('/');
    }
}
