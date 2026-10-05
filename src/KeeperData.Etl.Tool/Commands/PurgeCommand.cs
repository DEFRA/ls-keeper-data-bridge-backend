using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.DependencyInjection;
using System.CommandLine;

namespace KeeperData.Etl.Tool.Commands;

/// <summary>Clears staging artefacts so a run starts from the source files again.</summary>
internal static class PurgeCommand
{
    public static Command Create()
    {
        var stage = new Option<string?>("--stage")
        {
            Description = $"Clear from this stage downstream: {EtlStageCascade.Names}. Omit to clear them all."
        };

        var confirm = new Option<bool>("--yes") { Description = "Skip the confirmation prompt." };

        var command = new Command("purge", "Delete staging artefacts so the next run rebuilds them.")
        {
            CommonOptions.Staging,
            stage,
            confirm
        };

        command.SetAction(async (parseResult, cancellationToken) =>
        {
            using var host = EtlToolHost.Build(parseResult, quiet: true);

            var requested = parseResult.GetValue(stage);
            var selected = EtlStageCascade.From(requested);

            if (selected.Count == 0)
            {
                Console.Error.WriteLine($"Unknown stage '{requested}'. Expected one of: {EtlStageCascade.Names}.");
                return 2;
            }

            if (!parseResult.GetValue(confirm))
            {
                Console.Write($"Delete every object in {string.Join(", ", selected)}? [y/N] ");

                if (!string.Equals(Console.ReadLine()?.Trim(), "y", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Cancelled.");
                    return 0;
                }
            }

            var provider = host.Services.GetRequiredService<IEtlPipelineStorageProvider>();
            var cleared = await EtlStageCascade.ClearAsync(provider, selected, Report, cancellationToken);
            var store = host.Services.GetRequiredService<IEtlImportStatusStore>();

            if (cleared.ClearedStages.Count > 0)
            {
                await store.RecordPurgeAsync(
                    new EtlPurgeRecord(Guid.NewGuid(), BlobStorageSources.External, null, cleared.ClearedStages, cleared.Deleted),
                    cancellationToken);
            }

            if (cleared.Error is not null)
            {
                Console.Error.WriteLine(cleared.Error);

                if (cleared.IsPartial)
                {
                    Console.Error.WriteLine(
                        $"{cleared.Deleted} object(s) were already deleted from {string.Join(", ", cleared.ClearedStages)}.");
                }

                return 1;
            }

            Console.WriteLine();
            Console.WriteLine($"{cleared.Deleted} object(s) deleted across {selected.Count} stage(s).");

            return 0;
        });

        return command;
    }

    internal static void Report(string folder, int deleted)
        => Console.WriteLine($"  {folder,-12} {deleted,6} object(s) deleted");
}
