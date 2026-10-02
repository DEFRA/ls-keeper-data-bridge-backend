using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;
using KeeperData.Etl.Tool.Configuration;
using KeeperData.Infrastructure.EtlPipeline.Views;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.CommandLine;

namespace KeeperData.Etl.Tool.Commands;

/// <summary>What the tool resolved its settings to, and whether the things a run needs are actually
/// there. Cheaper than discovering a missing salt six stages in.</summary>
internal static class ConfigCommand
{
    public static Command Create()
    {
        var check = new Option<bool>("--check")
        {
            Description = "Also list a few objects from the bucket, to prove the credentials and endpoint work."
        };

        var command = new Command("config", "Show the resolved configuration and check what a run needs.")
        {
            CommonOptions.Environment,
            CommonOptions.Staging,
            check
        };

        command.SetAction(async (parseResult, cancellationToken) =>
        {
            using var host = EtlToolHost.Build(parseResult, quiet: true);

            var options = host.Services.GetRequiredService<IOptions<EtlToolOptions>>().Value;
            var configuration = host.Services.GetRequiredService<IConfiguration>();

            Console.WriteLine($"Environment   : {options.Environment}");
            Console.WriteLine($"Bucket        : {EtlToolHost.BucketName(options.BucketVariable) ?? $"MISSING ({options.BucketVariable})"}");
            Console.WriteLine($"Endpoint      : {Value(options.EndpointVariable)}");
            Console.WriteLine($"Access key    : {Secret(options.AccessKeyVariable)}");
            Console.WriteLine($"Secret key    : {Secret(options.SecretKeyVariable)}");
            Console.WriteLine($"Staging path  : {Describe(options.StagingPath)}");
            Console.WriteLine($"History db    : {options.HistoryDatabasePath}");
            Console.WriteLine($"Memory limit  : {options.MemoryLimit ?? "(DuckDB default)"}");
            Console.WriteLine($"AesSalt       : {(string.IsNullOrWhiteSpace(configuration["AesSalt"]) ? "MISSING" : "set")}");

            var extension = host.Services.GetRequiredService<IOptions<DuckDbConfiguration>>().Value.SqliteExtensionPath;

            Console.WriteLine($"SQLite ext    : {Describe(extension, isFile: true)}");

            Console.WriteLine();
            Console.WriteLine("Datasets");

            foreach (var definition in host.Services.GetRequiredService<IDataSetDefinitions>().All)
            {
                Console.WriteLine($"  {definition.Name,-28} {definition.SourceKeyPattern ?? definition.FilePrefixFormat}");
            }

            if (!parseResult.GetValue(check)) return 0;

            Console.WriteLine();
            Console.WriteLine("Bucket check");

            try
            {
                var source = host.Services.GetRequiredService<IBlobStorageServiceFactory>().GetSourceExternal();
                var page = await source.ListPageAsync(pageSize: 5, cancellationToken: cancellationToken);

                Console.WriteLine($"  reachable - {page.Items.Count} key(s) sampled");

                foreach (var item in page.Items)
                {
                    Console.WriteLine($"    {item.Key}");
                }
            }
            catch (Exception exception)
            {
                Console.Error.WriteLine($"  unreachable - {exception.Message}");
                return 1;
            }

            return 0;
        });

        return command;
    }

    private static string Describe(string path, bool isFile = false)
    {
        if (string.IsNullOrWhiteSpace(path)) return "(not set)";

        var exists = isFile ? File.Exists(path) : Directory.Exists(path);

        return $"{path} {(exists ? "" : "(does not exist)")}".TrimEnd();
    }

    private static string Value(string variable)
        => Environment.GetEnvironmentVariable(variable) is { Length: > 0 } value
            ? value
            : $"MISSING ({variable})";

    /// <summary>Credentials are reported as present or absent, never echoed.</summary>
    private static string Secret(string variable)
        => Environment.GetEnvironmentVariable(variable) is { Length: > 0 }
            ? $"set ({variable})"
            : $"MISSING ({variable})";
}
