using KeeperData.Common.Exceptions;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace KeeperData.Bridge.Middleware;

public sealed class ExceptionHandlingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ExceptionHandlingMiddleware> _logger;
    private readonly string _traceHeader;

    public ExceptionHandlingMiddleware(
        RequestDelegate next,
        ILogger<ExceptionHandlingMiddleware> logger,
        IConfiguration cfg)
    {
        _next = next;
        _logger = logger;
        _traceHeader = cfg.GetValue<string>("TraceHeader") ?? "x-correlation-id"; //_traceHeader "x-cdp-request-id"
    }

    public async Task InvokeAsync(HttpContext context)
    {
        context.Request.Headers.TryGetValue(_traceHeader, out var headerValues);
        var correlationId = headerValues.FirstOrDefault() ?? context.TraceIdentifier;

        try
        {
            await _next(context);
        }
        catch (FluentValidation.ValidationException ex)
        {
            await HandleExceptionAsync(context, ex, correlationId, 422, "Unprocessable Content");
        }
        catch (NotFoundException ex)
        {
            await HandleExceptionAsync(context, ex, correlationId, 404);
        }
        catch (DomainException ex)
        {
            await HandleExceptionAsync(context, ex, correlationId, 400);
        }
        catch (Exception ex)
        {
            await HandleExceptionAsync(context, ex, correlationId, 500, "An error occurred");
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
            var logMessage = $"ErrorId: {errorId} | CorrelationId: {correlationId} | StatusCode: {statusCode}";
            if (statusCode == 500)
                _logger.LogError(exception, logMessage);
            else
                _logger.LogInformation(exception, logMessage);
        }

        string resolvedTitle = title
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
        var jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true
        };

        var json = JsonSerializer.Serialize(problemDetails, jsonOptions);
        return context.Response.WriteAsync(json);
    }
}