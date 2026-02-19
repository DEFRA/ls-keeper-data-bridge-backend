using KeeperData.Bridge.Controllers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace KeeperData.Bridge.Filters;

/// <summary>
/// Global exception filter that handles <see cref="OperationCanceledException"/>
/// by returning a 499 (Client Closed Request) response with a standard error body.
/// </summary>
public class OperationCancelledExceptionFilter(ILogger<OperationCancelledExceptionFilter> logger) : IExceptionFilter
{
    public void OnException(ExceptionContext context)
    {
        if (context.Exception is not OperationCanceledException)
        {
            return;
        }

        logger.LogWarning("Request was cancelled");

        context.Result = new ObjectResult(new ErrorResponse
        {
            Message = "Request was cancelled.",
            Timestamp = DateTime.UtcNow
        })
        {
            StatusCode = StatusCodes.Status499ClientClosedRequest
        };

        context.ExceptionHandled = true;
    }
}
