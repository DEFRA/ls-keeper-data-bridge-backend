using System.Text.Json;
using System.Text.Json.Serialization;
using FluentAssertions;
using KeeperData.Infrastructure;

namespace KeeperData.Infrastructure.Tests.Unit;

public class JsonDefaultsTests
{
    [Fact]
    public void DefaultOptionsWithStringEnumConversion_UsesCamelCase()
    {
        var options = JsonDefaults.DefaultOptionsWithStringEnumConversion;

        options.PropertyNamingPolicy.Should().Be(JsonNamingPolicy.CamelCase);
    }

    [Fact]
    public void DefaultOptionsWithStringEnumConversion_IsNotIndented()
    {
        var options = JsonDefaults.DefaultOptionsWithStringEnumConversion;

        options.WriteIndented.Should().BeFalse();
    }

    [Fact]
    public void DefaultOptionsWithStringEnumConversion_IncludesEnumConverter()
    {
        var options = JsonDefaults.DefaultOptionsWithStringEnumConversion;

        options.Converters.Should().Contain(c => c is JsonStringEnumConverter);
    }

    [Fact]
    public void DefaultOptionsWithStringEnumConversion_SerializesEnumAsString()
    {
        var options = JsonDefaults.DefaultOptionsWithStringEnumConversion;
        var testObject = new TestClass { Status = TestStatus.Active };

        var json = JsonSerializer.Serialize(testObject, options);

        json.Should().Contain("\"Active\"");
        json.Should().NotContain("1"); // Should not contain numeric value
    }

    [Fact]
    public void DefaultOptionsWithIndented_UsesCamelCase()
    {
        var options = JsonDefaults.DefaultOptionsWithIndented;

        options.PropertyNamingPolicy.Should().Be(JsonNamingPolicy.CamelCase);
    }

    [Fact]
    public void DefaultOptionsWithIndented_IsIndented()
    {
        var options = JsonDefaults.DefaultOptionsWithIndented;

        options.WriteIndented.Should().BeTrue();
    }

    [Fact]
    public void DefaultOptionsWithIndented_ProducesIndentedJson()
    {
        var options = JsonDefaults.DefaultOptionsWithIndented;
        var testObject = new TestClass { Status = TestStatus.Active, Name = "Test" };

        var json = JsonSerializer.Serialize(testObject, options);

        json.Should().Contain("\n"); // Should have newlines for indentation
    }

    [Fact]
    public void DefaultOptionsWithStringEnumConversion_CanBeSet()
    {
        var originalOptions = JsonDefaults.DefaultOptionsWithStringEnumConversion;
        var newOptions = new JsonSerializerOptions { WriteIndented = true };

        try
        {
            JsonDefaults.DefaultOptionsWithStringEnumConversion = newOptions;

            JsonDefaults.DefaultOptionsWithStringEnumConversion.Should().Be(newOptions);
        }
        finally
        {
            // Restore original
            JsonDefaults.DefaultOptionsWithStringEnumConversion = originalOptions;
        }
    }

    [Fact]
    public void DefaultOptionsWithIndented_CanBeSet()
    {
        var originalOptions = JsonDefaults.DefaultOptionsWithIndented;
        var newOptions = new JsonSerializerOptions { WriteIndented = false };

        try
        {
            JsonDefaults.DefaultOptionsWithIndented = newOptions;

            JsonDefaults.DefaultOptionsWithIndented.Should().Be(newOptions);
        }
        finally
        {
            // Restore original
            JsonDefaults.DefaultOptionsWithIndented = originalOptions;
        }
    }

    [Fact]
    public void DefaultOptionsWithStringEnumConversion_AppliesCamelCaseToProperties()
    {
        var options = JsonDefaults.DefaultOptionsWithStringEnumConversion;
        var testObject = new TestClass { Name = "Test" };

        var json = JsonSerializer.Serialize(testObject, options);

        json.Should().Contain("\"name\""); // camelCase
        json.Should().NotContain("\"Name\""); // not PascalCase
    }

    private class TestClass
    {
        public string Name { get; set; } = string.Empty;
        public TestStatus Status { get; set; }
    }

    private enum TestStatus
    {
        Inactive = 0,
        Active = 1
    }
}
