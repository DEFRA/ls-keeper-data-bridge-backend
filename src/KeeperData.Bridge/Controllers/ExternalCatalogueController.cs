using System.Diagnostics.CodeAnalysis;
using System.Text;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class ExternalCatalogueController(
    IExternalCatalogueServiceFactory ExternalCatalogueServiceFactory,
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IDataSetDefinitions dataSetDefinitions,
    ILogger<ExternalCatalogueController> logger) : ControllerBase
{
    /// <summary>
    /// Gets a plain text report of available files for a specified source type.
    /// </summary>
    /// <param name="sourceType">The source type - either 'internal' or 'external'</param>
    /// <param name="days">Number of days to look back for files</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>A plain text report of available files</returns>
    [HttpGet("files")]
    public async Task<IActionResult> GetFilesReport(
        [FromQuery] string sourceType,
        [FromQuery] int days,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sourceType))
        {
            return BadRequest("Source type is required.");
        }

        if (sourceType != BlobStorageSources.Internal && sourceType != BlobStorageSources.External)
        {
            return BadRequest($"Invalid source type. Must be '{BlobStorageSources.Internal}' or '{BlobStorageSources.External}'.");
        }

        var ExternalCatalogueService = ExternalCatalogueServiceFactory.CreateLegacy(sourceType);

        var fileSets = await ExternalCatalogueService.GetFileSetsAsync(days, cancellationToken);

        var report = GenerateReport(sourceType, fileSets, ExternalCatalogueService.GetType().Name);

        return Content(report, "text/plain", Encoding.UTF8);
    }

    /// <summary>
    /// Uploads a file to internal S3 storage. The filename must conform to one of the dataset definition patterns.
    /// </summary>
    /// <param name="objectKey">The object key for the file in S3, either the full key including its source folder or a bare file name when the dataset's folder is unambiguous</param>
    /// <param name="file">The file to upload (must be CSV format)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Success response or validation error</returns>
    [HttpPost("upload")]
    [Consumes("multipart/form-data")]
    [RequestSizeLimit(100_000_000)]
    public async Task<IActionResult> UploadFile(
        [FromQuery] string objectKey,
        IFormFile file,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Upload request received. ObjectKey: {ObjectKey}, ContentType: {ContentType}, FileName: {FileName}",
            objectKey, file?.ContentType, file?.FileName);

        if (string.IsNullOrWhiteSpace(objectKey))
        {
            return UnprocessableEntity(CreateValidationProblem("ObjectKey is required.", "ObjectKey"));
        }

        if (file == null || file.Length == 0)
        {
            return UnprocessableEntity(CreateValidationProblem("File is required and cannot be empty.", "File"));
        }

        var acceptedContentTypes = new[]
        {
            "text/csv",
            "application/csv",
            "text/comma-separated-values",
            "application/octet-stream",
            "text/plain"
        };

        var hasValidContentType = string.IsNullOrEmpty(file.ContentType) ||
                                 acceptedContentTypes.Contains(file.ContentType.ToLowerInvariant());

        var hasValidExtension = file.FileName?.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) == true;

        if (!hasValidContentType && !hasValidExtension)
        {
            var acceptedTypes = string.Join(", ", acceptedContentTypes);
            return UnprocessableEntity(CreateValidationProblem(
                $"Invalid file type. Expected CSV file with valid content type ({acceptedTypes}) or .csv extension. Received ContentType: {file.ContentType ?? "null"}, FileName: {file.FileName ?? "null"}",
                "File"));
        }

        var resolution = DataSetUploadKey.Resolve(dataSetDefinitions.All, objectKey);
        if (resolution.Key is null)
        {
            return UnprocessableEntity(CreateValidationProblem(resolution.ErrorMessage ?? "ObjectKey validation failed.", "ObjectKey"));
        }

        var blobStorageService = blobStorageServiceFactory.GetSourceInternal();

        byte[] fileContent;
        using (var memoryStream = new MemoryStream())
        {
            await file.CopyToAsync(memoryStream, cancellationToken);
            fileContent = memoryStream.ToArray();
        }

        await blobStorageService.UploadAsync(
            resolution.Key,
            fileContent,
            file.ContentType,
            cancellationToken: cancellationToken);

        return Ok(new
        {
            Message = "File uploaded successfully",
            ObjectKey = resolution.Key,
            Size = fileContent.Length,
            ContentType = file.ContentType
        });
    }

    /// <summary>
    /// Uploads raw file content to internal S3 storage. The filename must conform to one of the dataset definition patterns.
    /// Alternative endpoint for testing with raw file content.
    /// </summary>
    /// <param name="objectKey">The object key for the file in S3, either the full key including its source folder or a bare file name when the dataset's folder is unambiguous</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Success response or validation error</returns>
    [HttpPost("upload-raw")]
    [Consumes("text/csv", "application/csv", "text/comma-separated-values", "text/plain", "application/octet-stream")]
    [RequestSizeLimit(100_000_000)]
    public async Task<IActionResult> UploadRawFile(
        [FromQuery] string objectKey,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Raw upload request received. ObjectKey: {ObjectKey}, ContentType: {ContentType}",
            objectKey, Request.ContentType);

        if (string.IsNullOrWhiteSpace(objectKey))
        {
            return UnprocessableEntity(CreateValidationProblem("ObjectKey is required.", "ObjectKey"));
        }

        if (Request.ContentLength == 0)
        {
            return UnprocessableEntity(CreateValidationProblem("File content is required and cannot be empty.", "File"));
        }

        var resolution = DataSetUploadKey.Resolve(dataSetDefinitions.All, objectKey);
        if (resolution.Key is null)
        {
            return UnprocessableEntity(CreateValidationProblem(resolution.ErrorMessage ?? "ObjectKey validation failed.", "ObjectKey"));
        }

        var blobStorageService = blobStorageServiceFactory.GetSourceInternal();

        byte[] fileContent;
        using (var memoryStream = new MemoryStream())
        {
            await Request.Body.CopyToAsync(memoryStream, cancellationToken);
            fileContent = memoryStream.ToArray();
        }

        if (fileContent.Length == 0)
        {
            return UnprocessableEntity(CreateValidationProblem("File content cannot be empty.", "File"));
        }

        await blobStorageService.UploadAsync(
            resolution.Key,
            fileContent,
            Request.ContentType ?? "text/csv",
            cancellationToken: cancellationToken);

        return Ok(new
        {
            Message = "File uploaded successfully",
            ObjectKey = resolution.Key,
            Size = fileContent.Length,
            ContentType = Request.ContentType ?? "text/csv"
        });
    }

    private static string GenerateReport(string sourceType, IReadOnlyList<FileSet> fileSets, string footer)
    {
        var report = new StringBuilder();
        report.AppendLine($"FILE REPORT FOR SOURCE: {sourceType.ToUpperInvariant()}");
        report.AppendLine($"Generated on: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        report.AppendLine(new string('=', 60));
        report.AppendLine();

        if (!fileSets.Any())
        {
            report.AppendLine("No files found for the specified source type.");
            return report.ToString();
        }

        var totalFiles = fileSets.Sum(fs => fs.Files.Count());
        report.AppendLine($"Total datasets: {fileSets.Count}");
        report.AppendLine($"Total files: {totalFiles}");
        report.AppendLine();

        foreach (var fileSet in fileSets.OrderBy(fs => fs.Definition.Name))
        {
            report.AppendLine($"DATASET: {fileSet.Definition.Name}");
            report.AppendLine($"  File Pattern: {fileSet.Definition.FilePrefixFormat}");
            report.AppendLine($"  Date Pattern: {fileSet.Definition.DatePattern}");
            report.AppendLine($"  File Count: {fileSet.Files.Count()}");

            if (fileSet.Files.Any())
            {
                report.AppendLine("  Files:");
                foreach (var file in fileSet.Files.OrderBy(f => f.Timestamp))
                {
                    var sizeKB = file.StorageObject.Size / 1024.0;
                    var sizeDisplay = sizeKB < 1024 ? $"{sizeKB:F1} KB" : $"{sizeKB / 1024:F1} MB";

                    report.AppendLine($"    - {file.StorageObject.Key}");
                    report.AppendLine($"      Size: {sizeDisplay}");
                    report.AppendLine($"      Last Modified: {file.StorageObject.LastModified:yyyy-MM-dd HH:mm:ss} UTC");
                    report.AppendLine($"      ETag: {file.StorageObject.ETag}");
                    report.AppendLine($"      Timestamp: {file.Timestamp}");
                }
            }
            else
            {
                report.AppendLine("  No files found for this dataset.");
            }

            report.AppendLine();
        }

        report.AppendLine(footer);

        return report.ToString();
    }

    private ValidationProblemDetails CreateValidationProblem(string errorMessage, string fieldName)
    {
        var problemDetails = new ValidationProblemDetails
        {
            Status = 422,
            Title = "Validation Error",
            Detail = errorMessage,
            Instance = HttpContext.Request.Path
        };

        problemDetails.Errors.Add(fieldName, [errorMessage]);
        return problemDetails;
    }

}