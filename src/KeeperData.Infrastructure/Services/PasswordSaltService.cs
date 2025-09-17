using KeeperData.Core.Exceptions;
using KeeperData.Core.Interfaces;
using Microsoft.Extensions.Configuration;
using System.Text.RegularExpressions;

namespace KeeperData.Infrastructure.Services;
public partial class PasswordSaltService(IConfiguration configuration, TimeProvider timeProvider) : IPasswordSaltService
{
    private static readonly Regex DatePattern = DatePatternRegEx();
    private readonly IConfiguration _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    private readonly TimeProvider _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
    private readonly Random _random = new();

    public PasswordSalt Get(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
        {
            throw new ArgumentNullException(nameof(fileName));
        }

        var salt = _configuration["AesSalt"];
        if (string.IsNullOrWhiteSpace(salt))
        {
            throw new InvalidOperationException("AesSalt configuration value is missing or empty.");
        }

        var components = ParseFileName(fileName);

        var password = GeneratePassword(components);

        return new PasswordSalt(password, salt);
    }

    public string GenerateFileName()
    {
        var dateTime = _timeProvider.GetUtcNow();
        var dateString = dateTime.ToString("yyyy-MM-dd");
        var timeString = dateTime.ToString("HHmmss");
        
        var prefixes = new List<string>();
        for (int i = 0; i < 7; i++)
        {
            var length = _random.Next(2, 10);        
            prefixes.Add(GenerateRandomString(length));
        }
        
        var fileName = $"{string.Join("_", prefixes)}_{dateString}-{timeString}.csv";
        
        return fileName;
    }

    private string GenerateRandomString(int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var stringChars = new char[length];
        
        for (int i = 0; i < length; i++)
        {
            stringChars[i] = chars[_random.Next(chars.Length)];
        }
        
        return new string(stringChars);
    }

    private FileNameComponents ParseFileName(string fileName)
    {
        var lastDotIndex = fileName.LastIndexOf('.');
        var fileExtension = lastDotIndex > -1 ? fileName.Substring(lastDotIndex) : string.Empty;
        var fileNameWithoutExtension = lastDotIndex > -1 ? fileName.Substring(0, lastDotIndex) : fileName;

        if (!fileNameWithoutExtension.Contains('_'))
        {
            throw new InvalidFileNameFormatException(fileName);
        }

        var dateMatch = DatePattern.Match(fileNameWithoutExtension);
        if (!dateMatch.Success)
        {
            throw new InvalidFileNameFormatException(fileName);
        }

        var dateString = dateMatch.Value;
        var dateIndex = dateMatch.Index;

        var beforeDate = fileNameWithoutExtension.Substring(0, dateIndex).TrimEnd('_', '-');
        var afterDateStartIndex = dateIndex + dateString.Length;
        var afterDate = afterDateStartIndex < fileNameWithoutExtension.Length 
            ? fileNameWithoutExtension.Substring(afterDateStartIndex).TrimStart('_', '-') 
            : string.Empty;

        return new FileNameComponents(beforeDate, dateString, afterDate, fileExtension);
    }

    private string GeneratePassword(FileNameComponents components)
    {
        var firstHalfParts = components.BeforeDate.Split('_');
        Array.Reverse(firstHalfParts);
        var part2 = string.Join("_", firstHalfParts);

        var part1 = components.DateString;

        var password = $"{part1}_{part2}";
        
        if (!string.IsNullOrEmpty(components.AfterDate))
        {
            var afterDateParts = components.AfterDate.Split('_');
            Array.Reverse(afterDateParts);
            password += "_" + string.Join("_", afterDateParts);
        }

        password += components.FileExtension;

        return password;
    }

    private record FileNameComponents(string BeforeDate, string DateString, string AfterDate, string FileExtension);

    [GeneratedRegex(@"\d{4}-\d{2}-\d{2}", RegexOptions.Compiled)]
    private static partial Regex DatePatternRegEx();
}