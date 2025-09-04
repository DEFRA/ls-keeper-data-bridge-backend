using Amazon.S3;
using KeeperData.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Infrastructure.Storage.Setup;

public class AwsS3HealthCheck(IS3ClientFactory s3ClientFactory) : IHealthCheck
{
    private readonly IS3ClientFactory _s3ClientFactory = s3ClientFactory;

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var results = new Dictionary<string, object>();
        var unhealthyClients = new List<string>();

        foreach (var clientName in _s3ClientFactory.GetRegisteredClientNames())
        {
            var client = _s3ClientFactory.GetClient(clientName);
            var bucketName = _s3ClientFactory.GetClientBucketName(clientName);

            try
            {
                var response = await client.GetBucketAclAsync(new Amazon.S3.Model.GetBucketAclRequest
                {
                    BucketName = bucketName
                }, cancellationToken);

                if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    results[clientName] = new
                    {
                        Bucket = bucketName,
                        Status = "Healthy",
                        Owner = response.Owner?.DisplayName ?? "Unknown"
                    };
                }
                else
                {
                    results[clientName] = new
                    {
                        Bucket = bucketName,
                        Status = $"Degraded (Status: {response.HttpStatusCode})"
                    };
                    unhealthyClients.Add(clientName);
                }
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                results[clientName] = new
                {
                    Bucket = bucketName,
                    Status = "Unhealthy (Bucket not found)",
                    Exception = ex.Message
                };
                unhealthyClients.Add(clientName);
            }
            catch (Exception ex)
            {
                results[clientName] = new
                {
                    Bucket = bucketName,
                    Status = "Unhealthy (Exception)",
                    Exception = ex.Message
                };
                unhealthyClients.Add(clientName);
            }
        }

        if (unhealthyClients.Count == 0)
        {
            return HealthCheckResult.Healthy("All S3 buckets are reachable", results);
        }

        return HealthCheckResult.Unhealthy($"Some S3 buckets failed: {string.Join(", ", unhealthyClients)}", data: results);
    }
}