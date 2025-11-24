using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Timeout;

namespace KeeperData.Core.Database.Resilience;

/// <summary>
/// Factory for creating Polly resilience pipelines for MongoDB operations.
/// Handles retry logic, circuit breaker, and timeout policies for transient failures.
/// </summary>
public static class MongoResiliencePipelineFactory
{
    /// <summary>
    /// Creates a resilience pipeline for MongoDB operations with retry, circuit breaker, and timeout.
    /// </summary>
    /// <typeparam name="T">The result type of the MongoDB operation</typeparam>
    /// <param name="config">Resilience configuration settings</param>
    /// <param name="logger">Logger for diagnostics</param>
    /// <param name="operationName">Name of the operation for logging context</param>
    /// <returns>Configured resilience pipeline</returns>
    public static ResiliencePipeline<T> Create<T>(
        Configuration.MongoResilienceConfig config,
        ILogger logger,
        string operationName)
    {
        var pipelineBuilder = new ResiliencePipelineBuilder<T>();

        // Add retry strategy for transient failures
        pipelineBuilder.AddRetry(new RetryStrategyOptions<T>
        {
            MaxRetryAttempts = config.MaxRetryAttempts,
            Delay = TimeSpan.FromMilliseconds(config.InitialDelayMs),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = config.UseJitter,
            ShouldHandle = new PredicateBuilder<T>()
                .Handle<MongoConnectionException>()
                .Handle<MongoExecutionTimeoutException>()
                .Handle<TimeoutException>()
                .Handle<MongoException>(ex =>
                    // Retry on network-related errors
                    ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase) ||
                    ex.Message.Contains("connection pool", StringComparison.OrdinalIgnoreCase) ||
                    ex.InnerException is System.Net.Sockets.SocketException),
            OnRetry = args =>
            {
                // Log at Debug level to avoid log spam, Warning only on final failure
                var logLevel = args.AttemptNumber < config.MaxRetryAttempts ? LogLevel.Debug : LogLevel.Warning;

                if (logLevel == LogLevel.Debug)
                {
                    logger.LogDebug(
                        args.Outcome.Exception,
                        "[MongoDB Resilience] {OperationName} failed (attempt {AttemptNumber}/{MaxRetryAttempts}). Retrying after {RetryDelay}ms",
                        operationName,
                        args.AttemptNumber,
                        config.MaxRetryAttempts,
                        args.RetryDelay.TotalMilliseconds);
                }
                else
                {
                    logger.LogWarning(
                        args.Outcome.Exception,
                        "[MongoDB Resilience] {OperationName} failed (attempt {AttemptNumber}/{MaxRetryAttempts}). Retrying after {RetryDelay}ms",
                        operationName,
                        args.AttemptNumber,
                        config.MaxRetryAttempts,
                        args.RetryDelay.TotalMilliseconds);
                }

                return ValueTask.CompletedTask;
            }
        });

        // Add circuit breaker if enabled
        if (config.EnableCircuitBreaker)
        {
            pipelineBuilder.AddCircuitBreaker(new CircuitBreakerStrategyOptions<T>
            {
                FailureRatio = config.CircuitBreakerFailureRatio,
                MinimumThroughput = config.CircuitBreakerMinimumThroughput,
                SamplingDuration = TimeSpan.FromSeconds(config.CircuitBreakerBreakDurationSeconds),
                BreakDuration = TimeSpan.FromSeconds(config.CircuitBreakerBreakDurationSeconds),
                ShouldHandle = new PredicateBuilder<T>()
                    .Handle<MongoConnectionException>()
                    .Handle<MongoExecutionTimeoutException>()
                    .Handle<TimeoutException>(),
                OnOpened = args =>
                {
                    logger.LogError(
                        "[MongoDB Resilience] Circuit breaker OPENED for {OperationName}. " +
                        "Too many failures detected. Breaking for {BreakDuration}s",
                        operationName,
                        config.CircuitBreakerBreakDurationSeconds);
                    return ValueTask.CompletedTask;
                },
                OnClosed = args =>
                {
                    logger.LogInformation(
                        "[MongoDB Resilience] Circuit breaker CLOSED for {OperationName}. Resuming normal operations",
                        operationName);
                    return ValueTask.CompletedTask;
                },
                OnHalfOpened = args =>
                {
                    logger.LogInformation(
                        "[MongoDB Resilience] Circuit breaker HALF-OPEN for {OperationName}. Testing if service recovered",
                        operationName);
                    return ValueTask.CompletedTask;
                }
            });
        }

        // Add timeout as the outer layer
        pipelineBuilder.AddTimeout(TimeSpan.FromSeconds(config.TimeoutSeconds));

        return pipelineBuilder.Build();
    }

    /// <summary>
    /// Creates a resilience pipeline for void/async Task MongoDB operations.
    /// </summary>
    public static ResiliencePipeline CreateForVoid(
        Configuration.MongoResilienceConfig config,
        ILogger logger,
        string operationName)
    {
        var pipelineBuilder = new ResiliencePipelineBuilder();

        // Add retry strategy
        pipelineBuilder.AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = config.MaxRetryAttempts,
            Delay = TimeSpan.FromMilliseconds(config.InitialDelayMs),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = config.UseJitter,
            ShouldHandle = new PredicateBuilder()
                .Handle<MongoConnectionException>()
                .Handle<MongoExecutionTimeoutException>()
                .Handle<TimeoutException>()
                .Handle<MongoException>(ex =>
                    ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase) ||
                    ex.Message.Contains("connection pool", StringComparison.OrdinalIgnoreCase) ||
                    ex.InnerException is System.Net.Sockets.SocketException),
            OnRetry = args =>
            {
                var logLevel = args.AttemptNumber < config.MaxRetryAttempts ? LogLevel.Debug : LogLevel.Warning;

                if (logLevel == LogLevel.Debug)
                {
                    logger.LogDebug(
                        args.Outcome.Exception,
                        "[MongoDB Resilience] {OperationName} failed (attempt {AttemptNumber}/{MaxRetryAttempts}). Retrying after {RetryDelay}ms",
                        operationName,
                        args.AttemptNumber,
                        config.MaxRetryAttempts,
                        args.RetryDelay.TotalMilliseconds);
                }
                else
                {
                    logger.LogWarning(
                        args.Outcome.Exception,
                        "[MongoDB Resilience] {OperationName} failed (attempt {AttemptNumber}/{MaxRetryAttempts}). Retrying after {RetryDelay}ms",
                        operationName,
                        args.AttemptNumber,
                        config.MaxRetryAttempts,
                        args.RetryDelay.TotalMilliseconds);
                }

                return ValueTask.CompletedTask;
            }
        });

        // Add circuit breaker if enabled
        if (config.EnableCircuitBreaker)
        {
            pipelineBuilder.AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = config.CircuitBreakerFailureRatio,
                MinimumThroughput = config.CircuitBreakerMinimumThroughput,
                SamplingDuration = TimeSpan.FromSeconds(config.CircuitBreakerBreakDurationSeconds),
                BreakDuration = TimeSpan.FromSeconds(config.CircuitBreakerBreakDurationSeconds),
                ShouldHandle = new PredicateBuilder()
                    .Handle<MongoConnectionException>()
                    .Handle<MongoExecutionTimeoutException>()
                    .Handle<TimeoutException>(),
                OnOpened = args =>
                {
                    logger.LogError(
                        "[MongoDB Resilience] Circuit breaker OPENED for {OperationName}. Breaking for {BreakDuration}s",
                        operationName,
                        config.CircuitBreakerBreakDurationSeconds);
                    return ValueTask.CompletedTask;
                },
                OnClosed = args =>
                {
                    logger.LogInformation(
                        "[MongoDB Resilience] Circuit breaker CLOSED for {OperationName}",
                        operationName);
                    return ValueTask.CompletedTask;
                },
                OnHalfOpened = args =>
                {
                    logger.LogInformation(
                        "[MongoDB Resilience] Circuit breaker HALF-OPEN for {OperationName}",
                        operationName);
                    return ValueTask.CompletedTask;
                }
            });
        }

        pipelineBuilder.AddTimeout(TimeSpan.FromSeconds(config.TimeoutSeconds));

        return pipelineBuilder.Build();
    }
}