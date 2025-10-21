using KeeperData.Bridge.Config;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Options;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Filters;

[ExcludeFromCodeCoverage]
public class FeatureFlagAttribute(string featureName) : ActionFilterAttribute
{
    public override void OnActionExecuting(ActionExecutingContext context)
    {
        var featureFlags = context.HttpContext.RequestServices.GetRequiredService<IOptions<FeatureFlags>>();

        var isEnabled = featureName switch
        {
            _ => false
        };

        if (!isEnabled)
        {
            context.Result = new NotFoundResult();
            return;
        }

        base.OnActionExecuting(context);
    }
}