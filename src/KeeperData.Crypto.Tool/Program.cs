using KeeperData.Core.Crypto;
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

var rootCommand = new RootCommand("KeeperData Crypto Tool - File encryption/decryption utility");

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

var getPasswordCommand = new Command("get-password", "Get password for a filename")
{
    new Option<string>("--filename") { Description = "Filename to get password for", Required = true }
};

getPasswordCommand.SetAction((parseResult) =>
{
    var filename = parseResult.GetValue<string>("--filename")!;
    var passwordSaltService = host.Services.GetRequiredService<IPasswordSaltService>();

    try
    {
        var passwordSalt = passwordSaltService.Get(filename);
        Console.WriteLine($"Password: {passwordSalt.Password}");
        // Console.WriteLine($"Salt: {passwordSalt.Salt}"); // ignore salt, we only use fake values in this tool.
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error getting password for filename '{filename}': {ex.Message}");
        Environment.Exit(1);
    }
});

var generateFilenameCommand = new Command("generate-filename", "Generate a compliant filename");

generateFilenameCommand.SetAction((parseResult) =>
{
    var passwordSaltService = host.Services.GetRequiredService<IPasswordSaltService>();

    try
    {
        var filename = passwordSaltService.GenerateFileName();
        Console.WriteLine($"Generated filename: {filename}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error generating filename: {ex.Message}");
        Environment.Exit(1);
    }
});

rootCommand.Add(encryptCommand);
rootCommand.Add(decryptCommand);
rootCommand.Add(getPasswordCommand);
rootCommand.Add(generateFilenameCommand);

var parseResult = rootCommand.Parse(args);
return await parseResult.InvokeAsync();

