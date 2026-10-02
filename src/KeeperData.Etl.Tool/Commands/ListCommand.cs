using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Etl.Tool.Rendering;
using Microsoft.Extensions.DependencyInjection;
using System.CommandLine;

namespace KeeperData.Etl.Tool.Commands;

/// <summary>The run history, newest first.</summary>
internal static class ListCommand
{
    public static Command Create()
    {
        var skip = new Option<int>("--skip") { Description = "Records to skip.", DefaultValueFactory = _ => 0 };
        var top = new Option<int>("--top") { Description = "Records to return.", DefaultValueFactory = _ => 20 };

        var status = new Option<string?>("--status")
        {
            Description = "Only operations with this status: Queued, Running, Succeeded, Failed, Rejected or Purged."
        };

        var command = new Command("list", "List ETL operations, most recent first.")
        {
            CommonOptions.Staging,
            skip,
            top,
            status,
            CommonOptions.Json
        };

        command.SetAction(async (parseResult, cancellationToken) =>
        {
            using var host = EtlToolHost.Build(parseResult, quiet: true);

            var store = host.Services.GetRequiredService<IEtlImportStatusStore>();
            var requested = parseResult.GetValue(status);

            var page = await store.ListAsync(parseResult.GetValue(skip), parseResult.GetValue(top), cancellationToken);

            if (!string.IsNullOrWhiteSpace(requested))
            {
                var matching = page.Imports
                    .Where(i => string.Equals(i.Status, requested, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                page = new EtlImportPage(matching, matching.Count);
            }

            if (parseResult.GetValue(CommonOptions.Json))
            {
                ImportRenderer.WriteJson(page);
            }
            else
            {
                ImportRenderer.WriteList(page, parseResult.GetValue(skip));
            }

            return 0;
        });

        return command;
    }
}
