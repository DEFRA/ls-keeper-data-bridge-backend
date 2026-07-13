using System.Text.Json;
using System.Text.Json.Serialization;
using KeeperData.Core.ETL.Export;
using KeeperData.Core.ETL.Models;
using KeeperData.Core.Storage;
using KeeperData.Core.Telemetry;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.ETL;

public class CphExportStatusService : ICphExportStatusService
{
    private readonly IBlobStorageServiceFactory _storageFactory;
    private readonly IApplicationMetrics _metrics;
    private readonly ILogger<CphExportStatusService> _logger;

    private const string ExportsPrefix = "exports/cphs/";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public CphExportStatusService(
        IBlobStorageServiceFactory storageFactory,
        IApplicationMetrics metrics,
        ILogger<CphExportStatusService> logger)
    {
        _storageFactory = storageFactory;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task<CphExportStatus> CreateAsync(Guid exportId, string sourceDuckDbPath, CancellationToken cancellationToken = default)
    {
        var status = new CphExportStatus
        {
            ExportId = exportId,
            Status = ExportStatusType.Queued,
            RequestedAt = DateTime.UtcNow,
            SourceDuckDbPath = sourceDuckDbPath
        };

        await PersistAsync(status, cancellationToken);

        _metrics.RecordCount("export.queued", 1);
        _logger.LogInformation("Created CPH export status {ExportId} with status Queued", exportId);

        return status;
    }

    public async Task<CphExportStatus?> GetAsync(Guid exportId, CancellationToken cancellationToken = default)
    {
        var storageService = _storageFactory.GetSourceInternal();
        var key = GetStatusKey(exportId);

        var exists = await storageService.ExistsAsync(key, cancellationToken);
        if (!exists)
        {
            return null;
        }

        var bytes = await storageService.DownloadAsync(key, cancellationToken);
        return JsonSerializer.Deserialize<CphExportStatus>(bytes, JsonOptions);
    }

    public async Task UpdateAsync(CphExportStatus status, CancellationToken cancellationToken = default)
    {
        await PersistAsync(status, cancellationToken);

        var metricName = status.Status switch
        {
            ExportStatusType.Succeeded => "export.succeeded",
            ExportStatusType.Failed => "export.failed",
            ExportStatusType.Running => "export.running",
            _ => null
        };

        if (metricName is not null)
        {
            _metrics.RecordCount(metricName, 1);
        }

        _logger.LogInformation("Updated CPH export status {ExportId} to {Status}", status.ExportId, status.Status);
    }

    public async Task<CphExportStatus?> GetLatestRunningAsync(CancellationToken cancellationToken = default)
    {
        var storageService = _storageFactory.GetSourceInternal();
        var objects = await storageService.ListAsync(ExportsPrefix, cancellationToken);

        var statusFiles = objects
            .Where(o => o.Key.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(o => o.LastModified)
            .Take(20)
            .ToList();

        foreach (var obj in statusFiles)
        {
            try
            {
                var bytes = await storageService.DownloadAsync(obj.Key, cancellationToken);
                var status = JsonSerializer.Deserialize<CphExportStatus>(bytes, JsonOptions);

                if (status is not null && status.Status is ExportStatusType.Queued or ExportStatusType.Running)
                {
                    return status;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize export status from {Key}", obj.Key);
            }
        }

        return null;
    }

    private async Task PersistAsync(CphExportStatus status, CancellationToken cancellationToken)
    {
        var storageService = _storageFactory.GetSourceInternal();
        var key = GetStatusKey(status.ExportId);
        var json = JsonSerializer.SerializeToUtf8Bytes(status, JsonOptions);

        await storageService.UploadAsync(key, json, "application/json", cancellationToken: cancellationToken);
    }

    private static string GetStatusKey(Guid exportId) => $"{ExportsPrefix}{exportId}.json";
}
