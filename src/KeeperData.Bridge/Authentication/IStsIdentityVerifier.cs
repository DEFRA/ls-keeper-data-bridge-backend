namespace KeeperData.Bridge.Authentication;

public interface IStsIdentityVerifier
{
    Task<StsVerificationResult> VerifyAsync(string presignedUrl, CancellationToken cancellationToken = default);
}

public sealed record StsCallerIdentity(
    string Arn,
    string Account,
    string UserId,
    string RoleName);

public sealed record StsVerificationResult
{
    public bool Succeeded { get; private init; }
    public StsCallerIdentity? Identity { get; private init; }
    public string? FailureReason { get; private init; }

    public static StsVerificationResult Success(StsCallerIdentity identity) => new()
    {
        Succeeded = true,
        Identity = identity
    };

    public static StsVerificationResult Fail(string reason) => new()
    {
        Succeeded = false,
        FailureReason = reason
    };
}