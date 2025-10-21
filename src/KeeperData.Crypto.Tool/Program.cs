using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Impl;
using KeeperData.Crypto.Tool.Services;
using KeeperData.Infrastructure.Crypto;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.CommandLine;

var builder = Host.CreateApplicationBuilder(args);
builder.Configuration.AddEnvironmentVariables();
builder.Configuration.AddInMemoryCollection([new KeyValuePair<string, string?>("AesSalt", "fakesalt")]);
builder.Services.AddLogging(config => config.AddConsole());
builder.Services.AddCrypto(builder.Configuration);

var host = builder.Build();

var rootCommand = new RootCommand("KeeperData Crypto Tool - File encryption/decryption and CSV generation utility");

var encryptCommand = new Command("encrypt", "Encrypt a file")
{
    new Option<string>("--input") { Description = "Input file path", Required = true },
    new Option<string>("--output") { Description = "Output file path", Required = true },
    new Option<string>("--password") { Description = "Encryption password", Required = true },
    new Option<string>("--salt") { Description = "Encryption salt", Required = true }
};

encryptCommand.SetAction(async (parseResult) =>
{
    var inputFile = parseResult.GetValue<string>("--input")!;
    var outputFile = parseResult.GetValue<string>("--output")!;
    var password = parseResult.GetValue<string>("--password")!;
    var salt = parseResult.GetValue<string>("--salt")!;

    var cryptoTransform = host.Services.GetRequiredService<IAesCryptoTransform>();

    Console.WriteLine($"Encrypting '{inputFile}' to '{outputFile}'...");

    try
    {
        await cryptoTransform.EncryptFileAsync(
            inputFile,
            outputFile,
            password,
            salt,
            (progress, status) =>
            {
                Console.Write($"\rProgress: {progress}% - {status}");
            });

        Console.WriteLine("\nEncryption completed successfully!");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\nEncryption failed: {ex.Message}");
        Environment.Exit(1);
    }
});

var decryptCommand = new Command("decrypt", "Decrypt a file")
{
    new Option<string>("--input") { Description = "Input file path", Required = true },
    new Option<string>("--output") { Description = "Output file path", Required = true },
    new Option<string>("--password") { Description = "Decryption password", Required = true },
    new Option<string>("--salt") { Description = "Decryption salt", Required = true }
};

decryptCommand.SetAction(async (parseResult) =>
{
    var inputFile = parseResult.GetValue<string>("--input")!;
    var outputFile = parseResult.GetValue<string>("--output")!;
    var password = parseResult.GetValue<string>("--password")!;
    var salt = parseResult.GetValue<string>("--salt")!;

    var cryptoTransform = host.Services.GetRequiredService<IAesCryptoTransform>();

    Console.WriteLine($"Decrypting '{inputFile}' to '{outputFile}'...");

    try
    {
        await cryptoTransform.DecryptFileAsync(
            inputFile,
            outputFile,
            password,
            salt,
            (progress, status) =>
            {
                Console.Write($"\rProgress: {progress}% - {status}");
            });

        Console.WriteLine("\nDecryption completed successfully!");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\nDecryption failed: {ex.Message}");
        Environment.Exit(1);
    }
});

// CSV Generation Commands
var generateCsvCommand = new Command("generate-csv", "Generate CSV files for all DataSetDefinitions")
{
    new Option<string>("--output") { Description = "Output directory for generated CSV files", Required = false },
    new Option<int>("--records") { Description = "Number of records in main files", Required = false },
    new Option<double>("--delta-percentage") { Description = "Percentage of records in delta files (0.0 to 1.0)", Required = false },
    new Option<double>("--update-percentage") { Description = "Percentage of delta records that are updates (vs deletes)", Required = false },
    new Option<int?>("--seed") { Description = "Random seed for reproducible generation", Required = false },
    new Option<int>("--columns") { Description = "Number of additional columns to generate", Required = false }
};

generateCsvCommand.SetAction(async (parseResult) =>
{
    var outputDir = parseResult.GetValue<string>("--output") ?? "generated-csv";
    var recordCount = parseResult.GetValue<int>("--records");
    if (recordCount == 0) recordCount = 1000;

    var deltaPercentage = parseResult.GetValue<double>("--delta-percentage");
    if (deltaPercentage == 0) deltaPercentage = 0.1;

    var updatePercentage = parseResult.GetValue<double>("--update-percentage");
    if (updatePercentage == 0) updatePercentage = 0.8;

    var seed = parseResult.GetValue<int?>("--seed");

    var columns = parseResult.GetValue<int>("--columns");
    if (columns == 0) columns = 5;

    var logger = host.Services.GetRequiredService<ILogger<Program>>();

    logger.LogInformation("CSV Generator");
    logger.LogInformation("=============");
    logger.LogInformation("Output Directory: {OutputDir}", outputDir);
    logger.LogInformation("Main File Records: {RecordCount}", recordCount);
    logger.LogInformation("Delta File Percentage: {DeltaPercentage:P0}", deltaPercentage);
    logger.LogInformation("Update Percentage: {UpdatePercentage:P0}", updatePercentage);
    logger.LogInformation("Random Seed: {Seed}", seed?.ToString() ?? "Random");
    logger.LogInformation("Additional Columns: {Columns}", columns);
    logger.LogInformation("");

    try
    {
        var options = new CsvGeneratorOptions
        {
            OutputDirectory = outputDir,
            MainFileRecordCount = recordCount,
            DeltaFileRecordPercentage = deltaPercentage,
            DeltaUpdatePercentage = updatePercentage,
            RandomSeed = seed,
            AdditionalColumnCount = columns
        };

        var generator = new CsvDataGenerator(options);
        var definitions = StandardDataSetDefinitionsBuilder.Build();

        logger.LogInformation("Generating CSV files for {Count} DataSetDefinitions...", definitions.All.Length);

        foreach (var definition in definitions.All)
        {
            logger.LogInformation("Generating files for DataSet: {Name}", definition.Name);
            await generator.GenerateForDefinitionAsync(definition);
            logger.LogInformation("  ✓ Main file created");
            logger.LogInformation("  ✓ Delta file created");
        }

        logger.LogInformation("");
        logger.LogInformation("Generation completed successfully!");
        logger.LogInformation("Files written to: {OutputDir}", Path.GetFullPath(outputDir));
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Generation failed: {Message}", ex.Message);
        Environment.Exit(1);
    }
});

var generateSingleCsvCommand = new Command("generate-csv-single", "Generate CSV files for a specific DataSetDefinition")
{
    new Option<string>("--name") { Description = "Name of the DataSetDefinition to generate", Required = true },
    new Option<string>("--output") { Description = "Output directory for generated CSV files", Required = false },
    new Option<int>("--records") { Description = "Number of records in main file", Required = false },
    new Option<double>("--delta-percentage") { Description = "Percentage of records in delta file (0.0 to 1.0)", Required = false },
    new Option<double>("--update-percentage") { Description = "Percentage of delta records that are updates (vs deletes)", Required = false },
    new Option<int?>("--seed") { Description = "Random seed for reproducible generation", Required = false },
    new Option<int>("--columns") { Description = "Number of additional columns to generate", Required = false }
};

generateSingleCsvCommand.SetAction(async (parseResult) =>
{
    var name = parseResult.GetValue<string>("--name")!;
    var outputDir = parseResult.GetValue<string>("--output") ?? "generated-csv";
    var recordCount = parseResult.GetValue<int>("--records");
    if (recordCount == 0) recordCount = 1000;

    var deltaPercentage = parseResult.GetValue<double>("--delta-percentage");
    if (deltaPercentage == 0) deltaPercentage = 0.1;

    var updatePercentage = parseResult.GetValue<double>("--update-percentage");
    if (updatePercentage == 0) updatePercentage = 0.8;

    var seed = parseResult.GetValue<int?>("--seed");

    var columns = parseResult.GetValue<int>("--columns");
    if (columns == 0) columns = 5;

    var logger = host.Services.GetRequiredService<ILogger<Program>>();

    try
    {
        var definitions = StandardDataSetDefinitionsBuilder.Build();
        var definition = definitions.All.FirstOrDefault(d => d.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

        if (definition == null)
        {
            logger.LogError("DataSetDefinition with name '{Name}' not found.", name);
            logger.LogInformation("Available definitions:");
            foreach (var def in definitions.All)
            {
                logger.LogInformation("  - {DefName}", def.Name);
            }
            Environment.Exit(1);
            return;
        }

        logger.LogInformation("Generating CSV files for DataSet: {Name}", definition.Name);

        var options = new CsvGeneratorOptions
        {
            OutputDirectory = outputDir,
            MainFileRecordCount = recordCount,
            DeltaFileRecordPercentage = deltaPercentage,
            DeltaUpdatePercentage = updatePercentage,
            RandomSeed = seed,
            AdditionalColumnCount = columns
        };

        var generator = new CsvDataGenerator(options);
        await generator.GenerateForDefinitionAsync(definition);

        logger.LogInformation("Generation completed successfully!");
        logger.LogInformation("Files written to: {OutputDir}", Path.GetFullPath(outputDir));
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Generation failed: {Message}", ex.Message);
        Environment.Exit(1);
    }
});

var listDatasetsCommand = new Command("list-datasets", "List all available DataSetDefinitions");

listDatasetsCommand.SetAction((parseResult) =>
{
    var logger = host.Services.GetRequiredService<ILogger<Program>>();
    var definitions = StandardDataSetDefinitionsBuilder.Build();

    logger.LogInformation("Available DataSetDefinitions:");
    logger.LogInformation("");

    foreach (var definition in definitions.All)
    {
        logger.LogInformation("Name: {Name}", definition.Name);
        logger.LogInformation("  File Prefix: {Prefix}", definition.FilePrefixFormat);
        logger.LogInformation("  Date Pattern: {Pattern}", definition.DatePattern);
        logger.LogInformation("  Primary Key Header: {Header}", definition.PrimaryKeyHeaderName);
        logger.LogInformation("  Change Type Header: {Header}", definition.ChangeTypeHeaderName);
        logger.LogInformation("");
    }

    return Task.FromResult(0);
});

rootCommand.Add(encryptCommand);
rootCommand.Add(decryptCommand);
rootCommand.Add(generateCsvCommand);
rootCommand.Add(generateSingleCsvCommand);
rootCommand.Add(listDatasetsCommand);

var parseResult = rootCommand.Parse(args);
return await parseResult.InvokeAsync();