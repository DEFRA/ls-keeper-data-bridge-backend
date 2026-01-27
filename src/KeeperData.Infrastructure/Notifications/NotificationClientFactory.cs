using System.Net;
using Microsoft.Extensions.Logging;
using Notify.Client;

namespace KeeperData.Infrastructure.Notifications;

/// <summary>
/// Factory for creating GOV.UK Notify clients with proxy configuration support.
/// </summary>
public class NotificationClientFactory : INotificationClientFactory
{
    private const string HttpProxyEnvVar = "HTTP_PROXY";
    
    private readonly ILogger<NotificationClientFactory> _logger;
    private readonly string? _proxyAddress;

    public NotificationClientFactory(ILogger<NotificationClientFactory> logger)
    {
        _logger = logger;
        _proxyAddress = Environment.GetEnvironmentVariable(HttpProxyEnvVar);
        
        if (!string.IsNullOrEmpty(_proxyAddress))
        {
            _logger.LogInformation("GOV.UK Notify client factory configured with proxy from {EnvVar}", HttpProxyEnvVar);
        }
        else
        {
            _logger.LogDebug("No proxy configured for GOV.UK Notify client (no {EnvVar} environment variable)", HttpProxyEnvVar);
        }
    }

    /// <inheritdoc />
    public NotificationClient Create(string apiKey)
    {
        if (string.IsNullOrEmpty(_proxyAddress))
        {
            return new NotificationClient(apiKey);
        }

        _logger.LogDebug("Creating GOV.UK Notify client with proxy");

        var proxy = new WebProxy(_proxyAddress);
        
        var handler = new HttpClientHandler
        {
            Proxy = proxy,
            UseProxy = true
        };

        var httpClient = new HttpClient(handler);
        
        return new NotificationClient(new HttpClientWrapper(httpClient), apiKey);
    }
}
