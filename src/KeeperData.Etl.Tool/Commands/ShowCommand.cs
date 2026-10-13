using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Etl.Tool.Rendering;
using Microsoft.Extensions.DependencyInjection;
using System.CommandLine;

namespace KeeperData.Etl.Tool.Commands;

/// <summary>One operation in full: its stages, what each dataset produced, the output keys, and the
/// failure if it had one.</summary>
internal static class ShowCommand
{
    public static Command Create()
    {
        var importId = new Argument<Guid>("import-id") { Description = "The import id, as listed by 'ketl list'." };

        var errorsOnly = new Option<bool>("--errors")
        {
            Description = "Print only the failure and its structured detail."
        };

        var command = new Command("show", "Show one ETL operation in detail.")
        {
            importId,
            CommonOptions.Staging,
            errorsOnly,
            CommonOptions.Json
        };

        command.SetAction(async (parseResult, cancellationToken) =>
        {
            using var host = EtlToolHost.Build(parseResult, quiet: true);

            var store = host.Services.GetRequiredService<IEtlImportStatusStore>();
            var id = parseResult.GetValue(importId);
            var import = await store.GetAsync(id, cancellationToken);

            if (import is null)
            {
                Console.Error.WriteLine($"No ETL operation with id {id}.");
                return 2;
            }

            if (parseResult.GetValue(CommonOptions.Json))
            {
                ImportRenderer.WriteJson(import);
            }
            else if (parseResult.GetValue(errorsOnly))
            {
                if (import.Error is null && import.ErrorDetail is null)
                {
                    Console.WriteLine($"Import {id} recorded no error. Status is {import.Status}.");
                }
                else
                {
                    ImportRenderer.WriteError(import);
                }
            }
            else
            {
                ImportRenderer.WriteDetail(import);
            }

            return 0;
        });

        return command;
    }
}
