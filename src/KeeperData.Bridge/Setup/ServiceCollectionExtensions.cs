using KeeperData.Application.Setup;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Bridge.Worker.Setup;
using KeeperData.Infrastructure.Database.Setup;
using KeeperData.Infrastructure.Messaging.Setup;
using KeeperData.Infrastructure.Storage.Setup;
using KeeperData.Infrastructure.ETL.Setup;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.Authorization;
using System.Text.Encodings.Web;
using System.Text.Json.Serialization;

namespace KeeperData.Bridge.Setup
{
    public static class ServiceCollectionExtensions
    {
        public static void ConfigureApi(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddDefaultAWSOptions(configuration.GetAWSOptions());

            services.ConfigureHealthChecks();

            services.ConfigureFeatureFlags(configuration);

            services.ConfigureAuthentication();

            services.ConfigureAuthorization();

            services.AddApplicationLayer();

            services.AddDatabaseDependencies(configuration);

            services.AddMessagingDependencies(configuration);

            services.AddStorageDependencies(configuration);

            services.AddEtlDependencies();

            services.AddCrypto(configuration);

            services.AddBackgroundJobDependencies(configuration);
        }

        private static void ConfigureHealthChecks(this IServiceCollection services)
        {
            services.AddHealthChecks();
        }

        private static void ConfigureFeatureFlags(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<FeatureFlags>(configuration.GetSection(FeatureFlags.SectionName));
        }

        private static void ConfigureAuthentication(this IServiceCollection services)
        {
            // UrlEncoder is required for authentication handlers
            services.AddSingleton<UrlEncoder>(_ => UrlEncoder.Default);

            services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = ApiKeyAuthenticationSchemeOptions.DefaultScheme;
                options.DefaultChallengeScheme = ApiKeyAuthenticationSchemeOptions.DefaultScheme;
            })
            .AddScheme<ApiKeyAuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(
                ApiKeyAuthenticationSchemeOptions.DefaultScheme,
                options => { });
        }

        private static void ConfigureAuthorization(this IServiceCollection services)
        {
            services.AddAuthorization(options =>
            {
                // Create a default policy that requires API key authentication
                options.DefaultPolicy = new AuthorizationPolicyBuilder()
                    .RequireAuthenticatedUser()
                    .AddAuthenticationSchemes(ApiKeyAuthenticationSchemeOptions.DefaultScheme)
                    .Build();

                // Create a policy for endpoints that should allow anonymous access
                options.AddPolicy("AllowAnonymous", policy =>
                {
                    policy.RequireAssertion(_ => true); // Always allow
                });
            });

            // Apply authorization globally to all controllers
            services.AddControllers(options =>
            {
                var policy = new AuthorizationPolicyBuilder()
                    .RequireAuthenticatedUser()
                    .AddAuthenticationSchemes(ApiKeyAuthenticationSchemeOptions.DefaultScheme)
                    .Build();
                options.Filters.Add(new AuthorizeFilter(policy));
            })
            .AddJsonOptions(opts =>
            {
                var enumConverter = new JsonStringEnumConverter();
                opts.JsonSerializerOptions.Converters.Add(enumConverter);
            });
        }
    }
}