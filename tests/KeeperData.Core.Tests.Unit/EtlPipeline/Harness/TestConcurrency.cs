using KeeperData.Core.EtlPipeline.Concurrency;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

/// <summary>The real fan-out rather than a stub, so a stage test exercises the concurrency the stage
/// actually runs with.</summary>
public static class TestConcurrency
{
    public static readonly EtlConcurrency Default = Of(new EtlConcurrencyOptions());

    public static EtlConcurrency Of(EtlConcurrencyOptions options)
        => new(Options.Create(options), NullLogger<EtlConcurrency>.Instance);
}
