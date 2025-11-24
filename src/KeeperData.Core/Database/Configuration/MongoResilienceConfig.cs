namespace KeeperData.Core.Database.Configuration;

/// <summary>
/// Configuration for MongoDB resilience policies including retry and circuit breaker settings.
/// </summary>
public record MongoResilienceConfig
{
    /// <summary>
    /// Maximum number of retry attempts for transient MongoDB failures.
    /// Default: 3 attempts
    /// </summary>
    public int MaxRetryAttempts { get; init; } = 3;

    /// <summary>
    /// Initial delay in milliseconds before the first retry attempt.
    /// Default: 500ms (aligns with existing batch throttling)
    /// </summary>
    public int InitialDelayMs { get; init; } = 500;

    /// <summary>
    /// Timeout in seconds for individual MongoDB operations.
    /// Default: 30 seconds
    /// </summary>
    public int TimeoutSeconds { get; init; } = 30;

    /// <summary>
    /// Enable circuit breaker pattern to prevent cascading failures.
    /// Default: true (recommended for production)
    /// </summary>
    public bool EnableCircuitBreaker { get; init; } = true;

    /// <summary>
    /// Circuit breaker failure ratio threshold (0.0 to 1.0).
    /// Circuit opens when this ratio of requests fail.
    /// Default: 0.5 (50% failure rate)
    /// </summary>
    public double CircuitBreakerFailureRatio { get; init; } = 0.5;

    /// <summary>
    /// Minimum number of requests before circuit breaker evaluates failure ratio.
    /// Default: 10 requests
    /// </summary>
    public int CircuitBreakerMinimumThroughput { get; init; } = 10;

    /// <summary>
    /// Duration in seconds the circuit stays open before attempting to close.
    /// Default: 30 seconds
    /// </summary>
    public int CircuitBreakerBreakDurationSeconds { get; init; } = 30;

    /// <summary>
    /// Use jitter in retry delays to prevent thundering herd.
    /// Default: true (recommended)
    /// </summary>
    public bool UseJitter { get; init; } = true;
}