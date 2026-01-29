using CsvHelper;
using CsvHelper.Configuration;
using FluentAssertions;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Impl;
using System.Globalization;

namespace KeeperData.Core.Tests.Unit.Reports.Impl;

public class CleanseReportCsvRecordTests
{
    [Fact]
    public void FromDomain_WithValidItem_CreatesCorrectRecord()
    {
        // Arrange
        var item = new CleanseReportItem
        {
            Id = "test-id",
            Code = "CTS_CPH_NOT_IN_SAM",
            Cph = "12/345/6789",
            CtsLidFullIdentifier = "AH-12/345/6789-001"
        };

        // Act
        var record = CleanseReportCsvRecord.FromDomain(item);

        // Assert
        record.CPH.Should().Be("12/345/6789");
        record.ErrorCode.Should().Be("CTS_CPH_NOT_IN_SAM");
    }

    [Fact]
    public void FromDomain_WithDifferentCodes_MapsCorrectly()
    {
        // Arrange
        var item = new CleanseReportItem
        {
            Id = "test-id-2",
            Code = "CTS_SAM_NO_EMAIL_ADDRESSES",
            Cph = "98/765/4321",
            CtsLidFullIdentifier = "AH-98/765/4321-002"
        };

        // Act
        var record = CleanseReportCsvRecord.FromDomain(item);

        // Assert
        record.CPH.Should().Be("98/765/4321");
        record.ErrorCode.Should().Be("CTS_SAM_NO_EMAIL_ADDRESSES");
    }
}

public class CleanseReportCsvMapTests
{
    [Fact]
    public void CsvMap_WritesCorrectHeaders()
    {
        // Arrange
        var records = new List<CleanseReportCsvRecord>
        {
            new() { CPH = "12/345/6789", ErrorCode = "TEST_CODE" }
        };

        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture));
        csv.Context.RegisterClassMap<CleanseReportCsvMap>();

        // Act
        csv.WriteRecords(records);
        var result = writer.ToString();

        // Assert
        result.Should().Contain("CPH");
        result.Should().Contain("ErrorCode");
        result.Should().Contain("12/345/6789");
        result.Should().Contain("TEST_CODE");
    }

    [Fact]
    public void CsvMap_ReadsCorrectly()
    {
        // Arrange
        var csvContent = "CPH,ErrorCode\n12/345/6789,TEST_CODE\n98/765/4321,ANOTHER_CODE";
        using var reader = new StringReader(csvContent);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
        csv.Context.RegisterClassMap<CleanseReportCsvMap>();

        // Act
        var records = csv.GetRecords<CleanseReportCsvRecord>().ToList();

        // Assert
        records.Should().HaveCount(2);
        records[0].CPH.Should().Be("12/345/6789");
        records[0].ErrorCode.Should().Be("TEST_CODE");
        records[1].CPH.Should().Be("98/765/4321");
        records[1].ErrorCode.Should().Be("ANOTHER_CODE");
    }

    [Fact]
    public void CsvMap_ColumnOrder_IsCorrect()
    {
        // Arrange
        var records = new List<CleanseReportCsvRecord>
        {
            new() { CPH = "TEST_CPH", ErrorCode = "TEST_ERROR" }
        };

        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture));
        csv.Context.RegisterClassMap<CleanseReportCsvMap>();

        // Act
        csv.WriteRecords(records);
        var lines = writer.ToString().Split(["\r\n", "\n"], StringSplitOptions.RemoveEmptyEntries);

        // Assert - CPH should be first column (index 0), ErrorCode second (index 1)
        var headers = lines[0].Split(',');
        headers[0].Should().Be("CPH");
        headers[1].Should().Be("ErrorCode");
    }
}
