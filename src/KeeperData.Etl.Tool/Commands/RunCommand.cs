using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using KeeperData.Etl.Tool.Configuration;
using KeeperData.Etl.Tool.Rendering;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.CommandLine;

namespace KeeperData.Etl.Tool.Commands;

/// <summary>Runs the pipeline the way the coordinator runs it: queue the import, then hand the
/// definition to the executor. Status is written by the observer, so a failure is on record before
/// this ever sees the exception.</summary>
internal static class RunCommand
{
    public static Command Create()
    {
        var dataset = new Option<string?>("--dataset")
        {
            Description = "Restrict the run to one dataset, e.g. cts_locations. Optional - omit it to process every configured dataset."
        };

        var runId = new Option<Guid?>("--run-id")
        {
            Description = "Use a specific import id, for re-running against an existing record."
        };

        var rebuild = new Option<bool>("--rebuild")
        {
            Description = "Rebuild everything from the source files: clears all staging artefacts first, " +
                          "so every file is re-downloaded, decrypted, normalised, optimised and merged."
        };

        var rebuildFrom = new Option<string?>("--rebuild-from")
        {
            Description = $"Rebuild from this stage downstream, keeping what precedes it: {EtlStageCascade.Names}."
        };

        var command = new Command("run", "Run the whole ETL pipeline, reading source files from the supplier's S3 bucket.")
        {
            CommonOptions.Environment,
            CommonOptions.Staging,
            CommonOptions.SeedTestData,
            dataset,
            runId,
            rebuild,
            rebuildFrom
        };

        command.SetAction(async (parseResult, cancellationToken) =>
        {
            using var host = EtlToolHost.Build(parseResult);

            var options = host.Services.GetRequiredService<IOptions<EtlToolOptions>>().Value;
            var configuration = host.Services.GetRequiredService<IConfiguration>();

            if (Invalid(options, configuration) is { } failure)
            {
                Console.Error.WriteLine(failure);
                return 2;
            }

            Directory.CreateDirectory(options.StagingPath);

            var importId = parseResult.GetValue(runId) ?? Guid.NewGuid();
            var selected = parseResult.GetValue(dataset);

            var store = host.Services.GetRequiredService<IEtlImportStatusStore>();

            Console.WriteLine($"Import  {importId}");
            Console.WriteLine($"Bucket  {EtlToolHost.BucketName(options.BucketVariable)} ({options.Environment})");
            Console.WriteLine($"Staging {options.StagingPath}");
            Console.WriteLine();

            if (await RebuildAsync(host, store, parseResult, rebuild, rebuildFrom, cancellationToken) is { } invalidStage)
            {
                Console.Error.WriteLine(invalidStage);
                return 2;
            }

            await store.CreateQueuedAsync(importId, BlobStorageSources.External, selected, cancellationToken);

            await using var scope = host.Services.CreateAsyncScope();

            var pipeline = scope.ServiceProvider.GetRequiredService<IEtlPipelineFactory>().Create();
            var executor = scope.ServiceProvider.GetRequiredService<IPipelineExecutor>();
            var context = new EtlPipelineContext(importId, BlobStorageSources.External, selected);

            var failed = false;

            try
            {
                await executor.RunAsync(pipeline, context, cancellationToken);
            }
            catch (PipelineExecutionException)
            {
                // The observer has already recorded why; reporting it is the detail below.
                failed = true;
            }

            var result = await store.GetAsync(importId, cancellationToken);

            Console.WriteLine();

            if (result is not null)
            {
                ImportRenderer.WriteDetail(result);
            }

            return failed ? 1 : 0;
        });

        return command;
    }

    /// <summary>Clears the artefacts a rebuild is meant to discard, and records the wipe beside the
    /// run that follows it. Returns a message when the named stage is not one, or null to proceed.</summary>
    private static async Task<string?> RebuildAsync(
        IHost host,
        IEtlImportStatusStore store,
        ParseResult parseResult,
        Option<bool> rebuild,
        Option<string?> rebuildFrom,
        CancellationToken cancellationToken)
    {
        var from = parseResult.GetValue(rebuildFrom);
        var all = parseResult.GetValue(rebuild);

        if (!all && string.IsNullOrWhiteSpace(from)) return null;

        // --rebuild is --rebuild-from the first stage; naming both avoids a bare stage name having
        // to be remembered for the common case.
        var stages = EtlStageCascade.From(all ? null : from);

        if (stages.Count == 0)
        {
            return $"Unknown stage '{from}'. Expected one of: {EtlStageCascade.Names}.";
        }

        Console.WriteLine($"Rebuilding {string.Join(", ", stages)}");

        var provider = host.Services.GetRequiredService<IEtlPipelineStorageProvider>();
        var cleared = await EtlStageCascade.ClearAsync(provider, stages, PurgeCommand.Report, cancellationToken);

        if (cleared.ClearedStages.Count > 0)
        {
            await store.RecordPurgeAsync(
                new EtlPurgeRecord(Guid.NewGuid(), BlobStorageSources.External, null, cleared.ClearedStages, cleared.Deleted),
                cancellationToken);
        }

        if (cleared.Error is not null)
        {
            return $"{cleared.Error}{Environment.NewLine}" +
                   (cleared.IsPartial
                       ? $"{string.Join(", ", cleared.ClearedStages)} were already cleared, so the staging area is " +
                         "now incomplete. Close whatever has the file open and run again to finish the rebuild."
                       : "Close whatever has the file open and run again; nothing has been cleared.");
        }

        Console.WriteLine();

        return null;
    }

    private static string? Invalid(EtlToolOptions options, IConfiguration configuration)
    {
        var missing = new[] { options.AccessKeyVariable, options.SecretKeyVariable, options.BucketVariable }
            .FirstOrDefault(variable => string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(variable)));

        if (missing is not null)
        {
            return $"{missing} is not set. Environment '{options.Environment}' needs its S3 credentials and bucket in the environment.";
        }

        if (string.IsNullOrWhiteSpace(configuration["AesSalt"]))
        {
            return "AesSalt is not set. The decrypt stage cannot derive a key without it; set the S3_AESSALT environment variable.";
        }

        return null;
    }
}
