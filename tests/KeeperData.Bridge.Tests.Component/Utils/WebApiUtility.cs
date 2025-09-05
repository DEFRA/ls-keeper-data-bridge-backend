using KeeperData.Infrastructure;
using System.Text;
using System.Text.Json;

namespace KeeperData.Bridge.Tests.Component.Utils;

public static class WebApiUtility
{
    public static HttpContent GetRequestContent(object model)
    {
        var httpContent = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(model));

        httpContent.Headers.Remove("Content-Type");
        httpContent.Headers.Add("Content-Type", "application/json");

        return httpContent;
    }

    public static StringContent GetErrorResultContent()
    {
        var result = new Exception("Error");

        var resultContent = new StringContent(
            content: JsonSerializer.Serialize(result, JsonDefaults.DefaultOptionsWithStringEnumConversion),
            encoding: Encoding.UTF8,
            mediaType: "application/json");

        return resultContent;
    }

    public static StringContent CreateResponseContent<T>(T response)
    {
        var resultContent = new StringContent(
            content: JsonSerializer.Serialize(response, JsonDefaults.DefaultOptionsWithStringEnumConversion),
            encoding: Encoding.UTF8,
            mediaType: "application/json");

        return resultContent;
    }
}