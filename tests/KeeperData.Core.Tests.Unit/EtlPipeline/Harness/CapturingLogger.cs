using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

/// <summary>Keeps the formatted messages a stage logged, for the cases where the log is the behaviour
/// rather than a side effect - a tolerated schema change, for instance, whose only trace is a
/// warning.</summary>
public sealed class CapturingLogger<T> : ILogger<T>
{
    private readonly List<(LogLevel Level, string Message)> _entries = [];

    public IReadOnlyList<string> Warnings =>
        [.. _entries.Where(entry => entry.Level == LogLevel.Warning).Select(entry => entry.Message)];

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);

        _entries.Add((logLevel, formatter(state, exception)));
    }
}
