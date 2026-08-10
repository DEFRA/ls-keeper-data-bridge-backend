using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Models;
using KeeperData.Core.ETL.Abstract;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

/// <summary>The dataset definitions the ETL pipeline is configured with.
///
/// Exposed so a caller can offer the same dataset names the trigger endpoint will accept, rather
/// than keeping its own copy of the list and drifting from this one.</summary>
[ApiController]
[Route("api/etl/datasets")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class EtlDatasetsController(
    IDataSetDefinitions dataSetDefinitions) : ControllerBase
{
    /// <summary>Every configured dataset, ordered by name.</summary>
    [HttpGet]
    [ProducesResponseType(typeof(EtlDatasetListResponse), StatusCodes.Status200OK)]
    public IActionResult GetDatasets()
        => Ok(new EtlDatasetListResponse
        {
            Datasets =
            [
                .. dataSetDefinitions.All
                    .OrderBy(d => d.Name, StringComparer.OrdinalIgnoreCase)
                    .Select(d => new EtlDatasetResponse
                    {
                        Name = d.Name,
                        FilePrefixFormat = d.FilePrefixFormat,
                        Format = d.Format.ToString(),
                        IngestionMode = d.IngestionMode.ToString()
                    })
            ]
        });
}
