using KeeperData.Core.Exceptions;
using KeeperData.Infrastructure;
using KeeperData.Core.Telemetry;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using System.Text.Json;

namespace KeeperData.Bridge.Middleware;

public sealed class ExceptionHandlingMiddleware(
        RequestDelegate next,
        ILogger<ExceptionHandlingMiddleware> logger,
        IConfiguration cfg,
        IApplicationMetrics metrics)
{
    private readonly RequestDelegate _next = next;
    private readonly ILogger<ExceptionHandlingMiddleware> _logger = logger;
    private readonly IApplicationMetrics _metrics = metrics;
    private readonly string _traceHeader = cfg.GetValue<string>("TraceHeader") ?? "x-correlation-id";

    public async Task InvokeAsync(HttpContext context)
    {
        context.Request.Headers.TryGetValue(_traceHeader, out var headerValues);
        var correlationId = headerValues.FirstOrDefault() ?? context.TraceIdentifier;
        var stopwatch = Stopwatch.StartNew();
        var endpoint = $"{context.Request.Method} {context.Request.Path}";

        try
        {
            await _next(context);

            // Record successful request metrics
            stopwatch.Stop();
            _metrics.RecordRequest("http_request", "success");
            _metrics.RecordDuration("http_request", stopwatch.ElapsedMilliseconds);
            _metrics.RecordCount("http_requests", 1,
                ("method", context.Request.Method),
                ("endpoint", context.Request.Path.Value ?? "unknown"),
                ("status_code", context.Response.StatusCode.ToString()),
                ("status", "success"));
        }
        catch (FluentValidation.ValidationException ex)
        {
            stopwatch.Stop();
            await HandleExceptionAsync(context, ex, correlationId, 422, "Unprocessable Content");
            RecordExceptionMetrics(ex, 422, endpoint, stopwatch.ElapsedMilliseconds, "validation_error");
        }
        catch (NotFoundException ex)
        {
            stopwatch.Stop();
            await HandleExceptionAsync(context, ex, correlationId, 404);
            RecordExceptionMetrics(ex, 404, endpoint, stopwatch.ElapsedMilliseconds, "not_found");
        }
        catch (DomainException ex)
        {
            stopwatch.Stop();
            await HandleExceptionAsync(context, ex, correlationId, 400);
            RecordExceptionMetrics(ex, 400, endpoint, stopwatch.ElapsedMilliseconds, "domain_error");
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await HandleExceptionAsync(context, ex, correlationId, 500, "An error occurred");
            RecordExceptionMetrics(ex, 500, endpoint, stopwatch.ElapsedMilliseconds, "internal_error");
        }
    }

    private Task HandleExceptionAsync(
        HttpContext context,
        Exception exception,
        string correlationId,
        int statusCode,
        string? title = null)
    {
        var errorId = Guid.NewGuid().ToString();

        using (_logger.BeginScope(new Dictionary<string, object> { ["trace.id"] = correlationId, ["error.id"] = errorId }))
        {
            const string logMessageTemplate = "ErrorId: {errorId} | CorrelationId: {correlationId} | StatusCode: {statusCode}";

            if (statusCode == 500)
            {
                _logger.LogError(exception, logMessageTemplate, errorId, correlationId, statusCode);
            }
            else
            {
                _logger.LogInformation(exception, logMessageTemplate, errorId, correlationId, statusCode);
            }
        }

        var resolvedTitle = title
            ?? (exception is DomainException de ? de.Title : "An error occurred");

        var problemDetails = new ProblemDetails
        {
            Title = resolvedTitle,
            Detail = exception.Message,
            Status = statusCode,
            Instance = context.Request.Path
        };

        if (exception is FluentValidation.ValidationException validationException)
        {
            var validationErrors = validationException.Errors
                .GroupBy(e => e.PropertyName)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(e => e.ErrorMessage).ToArray()
                );

            problemDetails.Extensions["errors"] = validationErrors;
            problemDetails.Detail = "One or more validation errors occurred. See the 'errors' field for details.";
        }

        problemDetails.Extensions["traceId"] = correlationId;
        problemDetails.Extensions["correlationId"] = correlationId;
        problemDetails.Extensions["errorId"] = errorId;

        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/json";

        var json = JsonSerializer.Serialize(problemDetails, JsonDefaults.DefaultOptionsWithIndented);
        return context.Response.WriteAsync(json);
    }

    private void RecordExceptionMetrics(Exception exception, int statusCode, string endpoint, double durationMs, string errorCategory)
    {
        try
        {
            // Record general error metrics
            _metrics.RecordRequest("http_request", "error");
            _metrics.RecordDuration("http_request", durationMs);

            // Record detailed error metrics
            _metrics.RecordCount("http_errors", 1,
                ("status_code", statusCode.ToString()),
                ("error_type", exception.GetType().Name),
                ("error_category", errorCategory),
                ("endpoint", endpoint));
            _metrics.RecordValue("error_response_time", durationMs,
                ("status_code", statusCode.ToString()),
                ("error_category", errorCategory));
            _metrics.RecordCount("exception_types", 1,
                ("exception_type", exception.GetType().Name),
                ("error_category", errorCategory));
            _metrics.RecordCount("http_status_codes", 1,
                ("status_code", statusCode.ToString()),
                ("status_class", GetStatusClass(statusCode)));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to record exception metrics for {ExceptionType}", exception.GetType().Name);
        }
    }

    private static string GetStatusClass(int statusCode)
    {
        return statusCode switch
        {
            >= 200 and < 300 => "2xx_success",
            >= 300 and < 400 => "3xx_redirection",
            >= 400 and < 500 => "4xx_client_error",
            >= 500 and < 600 => "5xx_server_error",
            _ => "unknown"
        };
    }
}