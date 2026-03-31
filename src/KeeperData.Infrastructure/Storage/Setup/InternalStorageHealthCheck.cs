using KeeperData.Infrastructure.Storage.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Infrastructure.Storage.Setup;

public class InternalStorageHealthCheck(StorageConfiguration storageConfiguration) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var provider = storageConfiguration.UseFileSystem ? "FS" : "S3";

        var data = new Dictionary<string, object>
        {
            ["Provider"] = provider
        };

        if (!storageConfiguration.UseFileSystem)
        {
            data["Bucket"] = storageConfiguration.InternalStorage.BucketName;

            return Task.FromResult(
                HealthCheckResult.Healthy($"Internal storage provider: {provider}", data));
        }

        var basePath = FileSystemBlobStorageServiceFactory.ResolveBasePath(storageConfiguration);
        data["BasePath"] = basePath;

        try
        {
            if (!Directory.Exists(basePath))
                Directory.CreateDirectory(basePath);

            var probe = Path.Combine(basePath, $".healthcheck-{Guid.NewGuid():N}");
            try
            {
                File.WriteAllBytes(probe, [0]);
            }
            finally
            {
                File.Delete(probe);
            }

            return Task.FromResult(
                HealthCheckResult.Healthy($"Internal storage provider: {provider}", data));
        }
        catch (Exception ex)
        {
            data["Error"] = ex.Message;

            return Task.FromResult(
                HealthCheckResult.Unhealthy($"Internal storage provider: {provider} — base path is not writable", ex, data));
        }
    }
}
