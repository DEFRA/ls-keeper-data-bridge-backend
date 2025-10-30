using Bogus;
using CsvHelper;
using CsvHelper.Configuration;
using KeeperData.Core.ETL.Impl;
using System.Globalization;

namespace KeeperData.Crypto.Tool.Services;

/// <summary>
/// Generates CSV data files for DataSetDefinitions.
/// </summary>
public class CsvDataGenerator
{
    private readonly CsvGeneratorOptions _options;
    private readonly Randomizer _randomizer;

    public CsvDataGenerator(CsvGeneratorOptions options)
    {
        _options = options;
        _randomizer = options.RandomSeed.HasValue
            ? new Randomizer(options.RandomSeed.Value)
            : new Randomizer();
    }

    /// <summary>
    /// Generates CSV files for all DataSetDefinitions.
    /// </summary>
    public async Task GenerateAllAsync(DataSetDefinitions dataSetDefinitions, CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(_options.OutputDirectory);

        foreach (var definition in dataSetDefinitions.All)
        {
            await GenerateForDefinitionAsync(definition, cancellationToken);
        }
    }

    /// <summary>
    /// Generates CSV files for a specific DataSetDefinition.
    /// </summary>
    public async Task GenerateForDefinitionAsync(DataSetDefinition definition, CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(_options.OutputDirectory);

        var timestamp = DateTime.Now.ToString(StandardDataSetDefinitionsBuilder.DateTimePattern);
        var filePrefix = string.Format(definition.FilePrefixFormat, timestamp);

        // Generate main file
        var mainFilePath = Path.Combine(_options.OutputDirectory, $"{filePrefix}.csv");
        var primaryKeys = await GenerateMainFileAsync(definition, mainFilePath, cancellationToken);

        // Generate delta file
        var deltaFilePath = Path.Combine(_options.OutputDirectory, $"{filePrefix}_delta.csv");
        await GenerateDeltaFileAsync(definition, deltaFilePath, primaryKeys, cancellationToken);
    }

    private async Task<List<string>> GenerateMainFileAsync(
        DataSetDefinition definition,
        string filePath,
        CancellationToken cancellationToken)
    {
        var primaryKeys = new List<string>();
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            Delimiter = "|" // Use pipe delimiter to match production format
        };

        await using var writer = new StreamWriter(filePath);
        await using var csv = new CsvWriter(writer, config);

        // Write headers
        var headers = GenerateHeaders(definition);
        foreach (var header in headers)
        {
            csv.WriteField(header);
        }
        await csv.NextRecordAsync();

        // Generate records
        var faker = new Faker();
        for (int i = 0; i < _options.MainFileRecordCount; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var compositeKeyParts = GenerateCompositePrimaryKey(faker, definition.PrimaryKeyHeaderNames.Length);
            var compositeKey = string.Join("__", compositeKeyParts);
            primaryKeys.Add(compositeKey);

            // Write primary key columns
            foreach (var keyPart in compositeKeyParts)
            {
                csv.WriteField(keyPart);
            }

            // Write change type (always 'I' for main file)
            csv.WriteField(ChangeType.Insert);

            // Write additional columns
            for (int j = 0; j < _options.AdditionalColumnCount; j++)
            {
                csv.WriteField(GenerateRandomValue(faker, j));
            }

            await csv.NextRecordAsync();
        }

        return primaryKeys;
    }

    private async Task GenerateDeltaFileAsync(
        DataSetDefinition definition,
        string filePath,
        List<string> availablePrimaryKeys,
        CancellationToken cancellationToken)
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            Delimiter = "|" // Use pipe delimiter to match production format
        };

        await using var writer = new StreamWriter(filePath);
        await using var csv = new CsvWriter(writer, config);

        // Write headers
        var headers = GenerateHeaders(definition);
        foreach (var header in headers)
        {
            csv.WriteField(header);
        }
        await csv.NextRecordAsync();

        // Select subset of primary keys for delta
        var deltaCount = (int)(_options.MainFileRecordCount * _options.DeltaFileRecordPercentage);
        var selectedKeys = _randomizer.Shuffle(availablePrimaryKeys).Take(deltaCount).ToList();

        var faker = new Faker();
        foreach (var compositeKey in selectedKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Determine if this is an update or delete
            var isUpdate = _randomizer.Double() < _options.DeltaUpdatePercentage;
            var changeType = isUpdate ? ChangeType.Update : ChangeType.Delete;

            // Split composite key back into parts and write each column
            var keyParts = compositeKey.Split("__");
            foreach (var keyPart in keyParts)
            {
                csv.WriteField(keyPart);
            }

            // Write change type
            csv.WriteField(changeType);

            // Write additional columns (only for updates, empty for deletes)
            for (int j = 0; j < _options.AdditionalColumnCount; j++)
            {
                if (isUpdate)
                {
                    csv.WriteField(GenerateRandomValue(faker, j));
                }
                else
                {
                    csv.WriteField(string.Empty);
                }
            }

            await csv.NextRecordAsync();
        }
    }

    private List<string> GenerateHeaders(DataSetDefinition definition)
    {
        var headers = new List<string>();

        // Add all primary key headers
        headers.AddRange(definition.PrimaryKeyHeaderNames);

        // Add change type header
        headers.Add(definition.ChangeTypeHeaderName);

        // Add meaningful column names based on data type
        var columnNames = new[]
        {
            "COMPANY_NAME",      // Column 0: Company names
            "LOCATION_CITY",     // Column 1: City names
            "QUANTITY",          // Column 2: Numbers
            "TRANSACTION_DATE",  // Column 3: Dates
            "STATUS_CODE"        // Column 4: Status/text
        };

        for (int i = 0; i < _options.AdditionalColumnCount; i++)
        {
            if (i < columnNames.Length)
            {
                headers.Add(columnNames[i]);
            }
            else
            {
                // For any additional columns beyond the predefined names
                headers.Add($"ATTRIBUTE_{i + 1}");
            }
        }

        return headers;
    }

    private List<string> GenerateCompositePrimaryKey(Faker faker, int keyCount)
    {
        var keys = new List<string>();
        for (int i = 0; i < keyCount; i++)
        {
            // Generate varied formats for different key parts
            if (i == 0)
            {
                // First key: format like '1254/KJB4HIH678'
                var number = faker.Random.Number(1000, 9999);
                var code = faker.Random.String2(10, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
                keys.Add($"{number}/{code}");
            }
            else
            {
                // Additional keys: simpler alphanumeric
                keys.Add(faker.Random.String2(8, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"));
            }
        }
        return keys;
    }

    private object GenerateRandomValue(Faker faker, int columnIndex)
    {
        // Vary the type of data based on column index for variety
        // Column names: COMPANY_NAME, LOCATION_CITY, QUANTITY, TRANSACTION_DATE, STATUS_CODE
        return (columnIndex % 5) switch
        {
            0 => faker.Company.CompanyName(),           // COMPANY_NAME
            1 => faker.Address.City(),                   // LOCATION_CITY
            2 => faker.Random.Number(1, 1000).ToString(), // QUANTITY
            3 => faker.Date.Past(2).ToString("yyyy-MM-dd"), // TRANSACTION_DATE
            _ => faker.Lorem.Word()                      // STATUS_CODE
        };
    }
}