using System.Text.Json;
using System.Text.Json.Serialization;

namespace KeeperData.Infrastructure;

public static class JsonDefaults
{
    private static JsonSerializerOptions s_defaultOptionsWithStringEnumConversion = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter() }
    };

    public static JsonSerializerOptions DefaultOptionsWithStringEnumConversion
    {
        get => s_defaultOptionsWithStringEnumConversion;
        set => s_defaultOptionsWithStringEnumConversion = value;
    }
}