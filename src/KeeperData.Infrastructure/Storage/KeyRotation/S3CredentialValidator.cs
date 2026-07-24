using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Logging;
using System.Net;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>
/// The external storage <see cref="AmazonS3Config"/> (respects the configured ServiceUrl /
/// LocalStack for local development), wrapped so it can be registered in DI without
/// clashing with the default <see cref="AmazonS3Config"/> singleton.
/// </summary>
public sealed record ExternalStorageS3Config(AmazonS3Config Value);

/// <summary>Outcome of validating candidate credentials against the external bucket.</summary>
public enum S3CredentialValidationOutcome
{
    /// <summary>The credentials authenticated and the probe succeeded.</summary>
    Valid,

    /// <summary>The credentials were rejected (403 / invalid key / bad signature).</summary>
    InvalidCredentials,

    /// <summary>A transient error (network/service) prevented a definitive answer.</summary>
    TransientError
}

/// <summary>Result of a credential validation probe.</summary>
public sealed record S3CredentialValidationResult(S3CredentialValidationOutcome Outcome, string? Detail = null);

/// <summary>
/// Validates candidate S3 credentials with a lightweight, nullipotent probe.
/// </summary>
public interface IS3CredentialValidator
{
    Task<S3CredentialValidationResult> ValidateAsync(
        string accessKeyId,
        string secretAccessKey,
        string bucketName,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Probes the bucket with <c>ListObjectsV2(MaxKeys=1)</c> using a short-lived candidate
/// client. The probe is read-only and does not depend on any particular object existing.
/// </summary>
public class S3CredentialValidator(
    ExternalStorageS3Config externalS3Config,
    ILogger<S3CredentialValidator> logger) : IS3CredentialValidator
{
    private const string LogPrefix = "[KeyRotation]";

    private static readonly string[] DeterministicAuthErrorCodes =
    [
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "AccessDenied",
        "ExpiredToken",
        "InvalidSecurity"
    ];

    public async Task<S3CredentialValidationResult> ValidateAsync(
        string accessKeyId,
        string secretAccessKey,
        string bucketName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(accessKeyId);
        ArgumentException.ThrowIfNullOrWhiteSpace(secretAccessKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        using var candidateClient = CreateCandidateClient(accessKeyId, secretAccessKey);

        try
        {
            await candidateClient.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucketName,
                MaxKeys = 1
            }, cancellationToken);

            return new S3CredentialValidationResult(S3CredentialValidationOutcome.Valid);
        }
        catch (AmazonS3Exception ex) when (IsDeterministicAuthFailure(ex))
        {
            logger.LogWarning(
                "{LogPrefix} Candidate credentials rejected by bucket {BucketName}: {ErrorCode} ({StatusCode})",
                LogPrefix, bucketName, ex.ErrorCode, (int)ex.StatusCode);

            return new S3CredentialValidationResult(
                S3CredentialValidationOutcome.InvalidCredentials,
                $"S3 rejected the credentials: {ex.ErrorCode} (HTTP {(int)ex.StatusCode})");
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "{LogPrefix} Transient error while validating candidate credentials against bucket {BucketName}",
                LogPrefix, bucketName);

            return new S3CredentialValidationResult(
                S3CredentialValidationOutcome.TransientError,
                $"Probe failed: {ex.GetType().Name}");
        }
    }

    /// <summary>
    /// Creates the short-lived client used for the probe. Virtual so unit tests can
    /// substitute a mocked <see cref="IAmazonS3"/> without touching the network.
    /// </summary>
    protected virtual IAmazonS3 CreateCandidateClient(string accessKeyId, string secretAccessKey) =>
        new AmazonS3Client(new BasicAWSCredentials(accessKeyId, secretAccessKey), externalS3Config.Value);

    private static bool IsDeterministicAuthFailure(AmazonS3Exception ex) =>
        ex.StatusCode == HttpStatusCode.Forbidden
        || (ex.ErrorCode is not null && DeterministicAuthErrorCodes.Contains(ex.ErrorCode, StringComparer.OrdinalIgnoreCase));
}
