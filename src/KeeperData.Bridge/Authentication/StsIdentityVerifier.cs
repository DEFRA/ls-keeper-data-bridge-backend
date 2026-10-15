using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using KeeperData.Bridge.Config;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Authentication;

public sealed class StsIdentityVerifier : IStsIdentityVerifier
{
    public const string HttpClientName = "StsVerify";

    private const string ExpectedAction = "GetCallerIdentity";
    private const string ExpectedAlgorithm = "AWS4-HMAC-SHA256";
    private const string ExpectedService = "sts";
    private const string ExpectedTerminator = "aws4_request";
    private const string AmzDateFormat = "yyyyMMdd'T'HHmmss'Z'";

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IOptionsMonitor<ApiAuthenticationOptions> _optionsMonitor;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<StsIdentityVerifier> _logger;
    private readonly ConcurrentDictionary<string, CachedIdentity> _cache = new(StringComparer.Ordinal);

    public StsIdentityVerifier(
        IHttpClientFactory httpClientFactory,
        IOptionsMonitor<ApiAuthenticationOptions> optionsMonitor,
        ILogger<StsIdentityVerifier> logger,
        TimeProvider? timeProvider = null)
    {
        _httpClientFactory = httpClientFactory;
        _optionsMonitor = optionsMonitor;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<StsVerificationResult> VerifyAsync(string presignedUrl, CancellationToken cancellationToken = default)
    {
        var stsOptions = _optionsMonitor.CurrentValue.Sts;

        if (!Uri.TryCreate(stsOptions.Endpoint, UriKind.Absolute, out var endpointUri)
            || (endpointUri.Scheme != Uri.UriSchemeHttps && endpointUri.Scheme != Uri.UriSchemeHttp))
        {
            _logger.LogError("Invalid STS endpoint configuration '{Endpoint}'", stsOptions.Endpoint);
            return StsVerificationResult.Fail("STS endpoint is not validly configured");
        }

        var now = _timeProvider.GetUtcNow();
        var validationError = ValidatePresignedUrl(presignedUrl, endpointUri, stsOptions, now, out var targetUri, out var signedExpiresAt);
        if (validationError != null)
        {
            _logger.LogWarning("Rejected STS presigned URL: {Reason}", validationError);
            return StsVerificationResult.Fail(validationError);
        }

        if (stsOptions.AllowedCallers.Count == 0)
        {
            _logger.LogWarning("Rejected STS authentication: no AllowedCallers configured");
            return StsVerificationResult.Fail("No allowed STS callers configured");
        }

        var cacheKey = ComputeCacheKey(presignedUrl);
        if (_cache.TryGetValue(cacheKey, out var cached) && now < cached.ExpiresAt)
        {
            return IsAllowed(cached.Identity, stsOptions.AllowedCallers)
                ? StsVerificationResult.Success(cached.Identity)
                : StsVerificationResult.Fail("Caller identity is not on the allowlist");
        }

        StsCallerIdentity identity;
        try
        {
            var client = _httpClientFactory.CreateClient(HttpClientName);
            using var response = await client.GetAsync(targetUri, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("STS GetCallerIdentity failed with HTTP {StatusCode}", (int)response.StatusCode);
                return StsVerificationResult.Fail($"STS rejected request with status {(int)response.StatusCode}");
            }

            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            var parsed = await ParseCallerIdentityAsync(stream, cancellationToken);
            if (parsed == null)
            {
                _logger.LogWarning("STS response did not contain valid GetCallerIdentity fields");
                return StsVerificationResult.Fail("Invalid STS GetCallerIdentity response");
            }

            identity = parsed;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error calling STS to verify caller identity");
            return StsVerificationResult.Fail("Failed to verify identity with STS");
        }

        if (!IsAllowed(identity, stsOptions.AllowedCallers))
        {
            _logger.LogWarning("STS caller '{Arn}' (account '{Account}', role '{RoleName}') is not on the allowlist",
                identity.Arn, identity.Account, identity.RoleName);
            return StsVerificationResult.Fail("Caller identity is not on the allowlist");
        }

        if (stsOptions.CacheTtlSeconds > 0)
        {
            var maxCacheUntil = now.AddSeconds(stsOptions.CacheTtlSeconds);
            var cacheUntil = signedExpiresAt < maxCacheUntil ? signedExpiresAt : maxCacheUntil;
            if (cacheUntil > now)
            {
                PruneExpiredEntries(now);
                _cache[cacheKey] = new CachedIdentity(identity, cacheUntil);
            }
        }

        return StsVerificationResult.Success(identity);
    }

    private static string? ValidatePresignedUrl(
        string presignedUrl,
        Uri endpointUri,
        StsAuthenticationOptions options,
        DateTimeOffset now,
        out Uri targetUri,
        out DateTimeOffset signedExpiresAt)
    {
        targetUri = null!;
        signedExpiresAt = default;

        if (string.IsNullOrWhiteSpace(presignedUrl) || !Uri.TryCreate(presignedUrl, UriKind.Absolute, out var uri))
        {
            return "Malformed presigned URL";
        }

        if (!string.Equals(uri.Scheme, endpointUri.Scheme, StringComparison.OrdinalIgnoreCase)
            || !string.Equals(uri.Host, endpointUri.Host, StringComparison.OrdinalIgnoreCase)
            || uri.Port != endpointUri.Port)
        {
            return "Presigned URL does not match configured STS endpoint";
        }

        if (!string.Equals(uri.AbsolutePath, "/", StringComparison.Ordinal)
            || !string.IsNullOrEmpty(uri.UserInfo)
            || !string.IsNullOrEmpty(uri.Fragment))
        {
            return "Presigned URL must target the root path with no userinfo or fragment";
        }

        var query = QueryHelpers.ParseQuery(uri.Query);

        if (!TryGetSingle(query, "Action", out var action) || !string.Equals(action, ExpectedAction, StringComparison.Ordinal))
        {
            return "Presigned URL must have Action=GetCallerIdentity";
        }

        if (!TryGetSingle(query, "X-Amz-Algorithm", out var algorithm) || !string.Equals(algorithm, ExpectedAlgorithm, StringComparison.Ordinal))
        {
            return "Presigned URL must use AWS4-HMAC-SHA256";
        }

        if (!TryGetSingle(query, "X-Amz-Credential", out var credential) || !IsValidCredentialScope(credential))
        {
            return "Presigned URL has an invalid X-Amz-Credential scope";
        }

        if (!TryGetSingle(query, "X-Amz-Date", out var amzDate)
            || !DateTimeOffset.TryParseExact(
                amzDate,
                AmzDateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var signedAt))
        {
            return "Presigned URL has an invalid X-Amz-Date";
        }

        if (!TryGetSingle(query, "X-Amz-Expires", out var expiresRaw)
            || !int.TryParse(expiresRaw, NumberStyles.None, CultureInfo.InvariantCulture, out var expiresSeconds)
            || expiresSeconds <= 0
            || expiresSeconds > options.MaxExpiresSeconds)
        {
            return $"Presigned URL X-Amz-Expires must be between 1 and {options.MaxExpiresSeconds} seconds";
        }

        var skew = TimeSpan.FromSeconds(Math.Max(0, options.ClockSkewSeconds));
        if (signedAt > now + skew)
        {
            return "Presigned URL X-Amz-Date is in the future";
        }

        signedExpiresAt = signedAt.AddSeconds(expiresSeconds);
        if (signedExpiresAt + skew <= now)
        {
            return "Presigned URL has expired";
        }

        if (!TryGetSingle(query, "X-Amz-SignedHeaders", out var signedHeaders)
            || !signedHeaders.Split(';', StringSplitOptions.RemoveEmptyEntries)
                .Contains("host", StringComparer.OrdinalIgnoreCase))
        {
            return "Presigned URL must sign the host header";
        }

        if (!TryGetSingle(query, "X-Amz-Signature", out var signature) || string.IsNullOrWhiteSpace(signature))
        {
            return "Presigned URL is missing X-Amz-Signature";
        }

        targetUri = uri;
        return null;
    }

    private static bool TryGetSingle(
        Dictionary<string, Microsoft.Extensions.Primitives.StringValues> query,
        string key,
        out string value)
    {
        if (query.TryGetValue(key, out var values) && values.Count == 1 && !string.IsNullOrWhiteSpace(values[0]))
        {
            value = values[0]!;
            return true;
        }

        value = string.Empty;
        return false;
    }

    private static bool IsValidCredentialScope(string credential)
    {
        var parts = credential.Split('/');
        return parts.Length == 5
            && !string.IsNullOrWhiteSpace(parts[0])
            && parts[1].Length == 8
            && !string.IsNullOrWhiteSpace(parts[2])
            && string.Equals(parts[3], ExpectedService, StringComparison.Ordinal)
            && string.Equals(parts[4], ExpectedTerminator, StringComparison.Ordinal);
    }

    private static async Task<StsCallerIdentity?> ParseCallerIdentityAsync(Stream stream, CancellationToken cancellationToken)
    {
        var settings = new XmlReaderSettings
        {
            Async = true,
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null
        };

        using var reader = XmlReader.Create(stream, settings);
        var doc = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
        var result = doc.Descendants().FirstOrDefault(e => e.Name.LocalName == "GetCallerIdentityResult");
        if (result == null) return null;

        var arn = result.Elements().FirstOrDefault(e => e.Name.LocalName == "Arn")?.Value;
        var account = result.Elements().FirstOrDefault(e => e.Name.LocalName == "Account")?.Value;
        var userId = result.Elements().FirstOrDefault(e => e.Name.LocalName == "UserId")?.Value;

        if (string.IsNullOrWhiteSpace(arn) || string.IsNullOrWhiteSpace(account) || string.IsNullOrWhiteSpace(userId))
        {
            return null;
        }

        return new StsCallerIdentity(arn, account, userId, ExtractRoleName(arn));
    }

    public static string ExtractRoleName(string arn)
    {
        var colonIndex = arn.LastIndexOf(':');
        var resource = colonIndex >= 0 && colonIndex < arn.Length - 1 ? arn[(colonIndex + 1)..] : arn;

        if (resource.StartsWith("assumed-role/", StringComparison.Ordinal))
        {
            var rest = resource["assumed-role/".Length..];
            var slash = rest.IndexOf('/');
            return slash > 0 ? rest[..slash] : rest;
        }

        if (resource.StartsWith("role/", StringComparison.Ordinal))
        {
            var rest = resource["role/".Length..];
            var lastSlash = rest.LastIndexOf('/');
            return lastSlash >= 0 && lastSlash < rest.Length - 1 ? rest[(lastSlash + 1)..] : rest;
        }

        if (resource.StartsWith("user/", StringComparison.Ordinal))
        {
            var rest = resource["user/".Length..];
            var lastSlash = rest.LastIndexOf('/');
            return lastSlash >= 0 && lastSlash < rest.Length - 1 ? rest[(lastSlash + 1)..] : rest;
        }

        return resource;
    }

    private static bool IsAllowed(StsCallerIdentity identity, List<AllowedStsCaller> allowedCallers)
    {
        foreach (var caller in allowedCallers)
        {
            if (string.IsNullOrWhiteSpace(caller.AccountId) || string.IsNullOrWhiteSpace(caller.RolePattern))
            {
                continue;
            }

            if (string.Equals(caller.AccountId, identity.Account, StringComparison.Ordinal)
                && MatchesGlob(identity.RoleName, caller.RolePattern))
            {
                return true;
            }
        }

        return false;
    }

    private static bool MatchesGlob(string value, string pattern)
    {
        if (pattern == "*") return true;
        var regexPattern = "^" + Regex.Escape(pattern).Replace("\\*", ".*") + "$";
        return Regex.IsMatch(value, regexPattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    private static string ComputeCacheKey(string presignedUrl)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(presignedUrl));
        return Convert.ToHexString(bytes);
    }

    private void PruneExpiredEntries(DateTimeOffset now)
    {
        if (_cache.Count < 256) return;
        foreach (var (key, entry) in _cache)
        {
            if (now >= entry.ExpiresAt)
            {
                _cache.TryRemove(key, out _);
            }
        }
    }

    private sealed record CachedIdentity(StsCallerIdentity Identity, DateTimeOffset ExpiresAt);
}