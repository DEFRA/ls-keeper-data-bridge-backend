using KeeperData.Core.Ingestion.Contracts;

namespace KeeperData.Bridge.Worker.NewPipelineUsage.Samples;

/// <summary>Sample decryptor: copies input to output unchanged so the demo flows end to end.
/// The real implementation streams through AesCryptoTransform with the salt from config.</summary>
public sealed class PassthroughFileDecryptor : IFileDecryptor
{
    public Task DecryptAsync(Stream input, Stream output, string password, CancellationToken cancellationToken)
        => input.CopyToAsync(output, cancellationToken);
}
