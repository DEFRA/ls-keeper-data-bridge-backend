using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage;
using KeeperData.Bridge.Filters;
using KeeperData.Bridge.Config;
using Microsoft.AspNetCore.Mvc;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.ETL.Abstract;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
[FeatureFlag(nameof(FeatureFlags.SourceDataController))]
public class SourceDataController(
    ISourceDataServiceFactory sourceDataServiceFactory,
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IDataSetDefinitions dataSetDefinitions) : ControllerBase
{
    /// <summary>
    /// Gets a plain text report of available files for a specified source type.
    /// </summary>
    /// <param name="sourceType">The source type - either 'internal' or 'external'</param>
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

        try
        {
            var sourceDataService = sourceDataServiceFactory.Create(sourceType);

            var fileSets = await sourceDataService.GetFileSetsAsync(days, cancellationToken);

            var report = GenerateReport(sourceType, fileSets, sourceDataService.ToString());

            return Content(report, "text/plain", Encoding.UTF8);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(ex.Message);
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"An error occurred while retrieving files: {ex.Message}");
        }
    }

    /// <summary>
    /// Uploads a file to internal S3 storage. The filename must conform to one of the dataset definition patterns.
    /// </summary>
    /// <param name="objectKey">The filename (object key) for the file in S3 - should not contain path separators</param>
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
        var logger = HttpContext.RequestServices.GetService<ILogger<SourceDataController>>();
        logger?.LogInformation("Upload request received. ObjectKey: {ObjectKey}, ContentType: {ContentType}, FileName: {FileName}",
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

        if (objectKey.Contains('/') || objectKey.Contains('\\'))
        {
            return UnprocessableEntity(CreateValidationProblem("ObjectKey should be a filename only, without path separators.", "ObjectKey"));
        }

        var fileName = objectKey;

        var validationResult = ValidateFileName(fileName);
        if (!validationResult.IsValid)
        {
            return UnprocessableEntity(CreateValidationProblem(validationResult.ErrorMessage ?? "Filename validation failed.", "FileName"));
        }

        try
        {
            var blobStorageService = blobStorageServiceFactory.GetSourceInternal();

            byte[] fileContent;
            using (var memoryStream = new MemoryStream())
            {
                await file.CopyToAsync(memoryStream, cancellationToken);
                fileContent = memoryStream.ToArray();
            }

            await blobStorageService.UploadAsync(
                objectKey,
                fileContent,
                file.ContentType,
                cancellationToken: cancellationToken);

            return Ok(new
            {
                Message = "File uploaded successfully",
                ObjectKey = objectKey,
                Size = fileContent.Length,
                ContentType = file.ContentType
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"An error occurred while uploading the file: {ex.Message}");
        }
    }

    /// <summary>
    /// Uploads raw file content to internal S3 storage. The filename must conform to one of the dataset definition patterns.
    /// Alternative endpoint for testing with raw file content.
    /// </summary>
    /// <param name="objectKey">The filename (object key) for the file in S3 - should not contain path separators</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Success response or validation error</returns>
    [HttpPost("upload-raw")]
    [Consumes("text/csv", "application/csv", "text/comma-separated-values", "text/plain", "application/octet-stream")]
    [RequestSizeLimit(100_000_000)]
    public async Task<IActionResult> UploadRawFile(
        [FromQuery] string objectKey,
        CancellationToken cancellationToken = default)
    {
        var logger = HttpContext.RequestServices.GetService<ILogger<SourceDataController>>();
        logger?.LogInformation("Raw upload request received. ObjectKey: {ObjectKey}, ContentType: {ContentType}",
            objectKey, Request.ContentType);

        if (string.IsNullOrWhiteSpace(objectKey))
        {
            return UnprocessableEntity(CreateValidationProblem("ObjectKey is required.", "ObjectKey"));
        }

        if (Request.ContentLength == 0)
        {
            return UnprocessableEntity(CreateValidationProblem("File content is required and cannot be empty.", "File"));
        }

        if (objectKey.Contains('/') || objectKey.Contains('\\'))
        {
            return UnprocessableEntity(CreateValidationProblem("ObjectKey should be a filename only, without path separators.", "ObjectKey"));
        }

        var validationResult = ValidateFileName(objectKey);
        if (!validationResult.IsValid)
        {
            return UnprocessableEntity(CreateValidationProblem(validationResult.ErrorMessage ?? "Filename validation failed.", "FileName"));
        }

        try
        {
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
                objectKey,
                fileContent,
                Request.ContentType ?? "text/csv",
                cancellationToken: cancellationToken);

            return Ok(new
            {
                Message = "File uploaded successfully",
                ObjectKey = objectKey,
                Size = fileContent.Length,
                ContentType = Request.ContentType ?? "text/csv"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"An error occurred while uploading the file: {ex.Message}");
        }
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
                foreach (var file in fileSet.Files.OrderByDescending(f => f.LastModified))
                {
                    var sizeKB = file.Size / 1024.0;
                    var sizeDisplay = sizeKB < 1024 ? $"{sizeKB:F1} KB" : $"{sizeKB / 1024:F1} MB";

                    report.AppendLine($"    - {file.Key}");
                    report.AppendLine($"      Size: {sizeDisplay}");
                    report.AppendLine($"      Last Modified: {file.LastModified:yyyy-MM-dd HH:mm:ss} UTC");
                    report.AppendLine($"      ETag: {file.ETag}");
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

    private FileValidationResult ValidateFileName(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
        {
            return new FileValidationResult(false, "Filename cannot be empty.");
        }

        var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);

        foreach (var definition in dataSetDefinitions.All)
        {
            if (IsFileNameMatchingDefinition(fileNameWithoutExtension, definition))
            {
                return new FileValidationResult(true, null);
            }
        }

        var validPatterns = dataSetDefinitions.All
            .Select(d => FormatExampleFileName(d))
            .ToList();

        var errorMessage = $"Filename '{fileName}' does not conform to any dataset definition pattern. " +
                          $"Valid patterns are: {string.Join(", ", validPatterns)}";

        return new FileValidationResult(false, errorMessage);
    }

    private static bool IsFileNameMatchingDefinition(string fileNameWithoutExtension, DataSetDefinition definition)
    {
        try
        {
            var prefixPattern = definition.FilePrefixFormat.Replace("{0}", "");
            var dateTimePattern = StandardDataSetDefinitionsBuilder.DateTimePattern;

            var expectedPattern = prefixPattern + dateTimePattern;

            var regexPattern = "^" + Regex.Escape(expectedPattern)
                .Replace("yyyyMMddHHmmss", @"\d{14}") + "$";

            var regex = new Regex(regexPattern, RegexOptions.IgnoreCase);

            if (!regex.IsMatch(fileNameWithoutExtension))
            {
                return false;
            }

            var dateTimeStart = prefixPattern.Length;
            if (dateTimeStart + 14 > fileNameWithoutExtension.Length)
            {
                return false;
            }

            var dateTimeString = fileNameWithoutExtension.Substring(dateTimeStart, 14);

            return DateTime.TryParseExact(
                dateTimeString,
                StandardDataSetDefinitionsBuilder.DateTimePattern,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out _);
        }
        catch
        {
            return false;
        }
    }

    private static string FormatExampleFileName(DataSetDefinition definition)
    {
        var prefix = definition.FilePrefixFormat.Replace("{0}", "");
        return $"{prefix}{StandardDataSetDefinitionsBuilder.DateTimePattern}.csv (e.g., {prefix}20241201123045.csv)";
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

    private record FileValidationResult(bool IsValid, string? ErrorMessage);
}