namespace KeeperData.Core.Exceptions;

public class InvalidFileNameFormatException : Exception
{
    public InvalidFileNameFormatException(string fileName) 
        : base($"The file name '{fileName}' does not contain the required format with underscores and date pattern (yyyy-MM-dd).")
    {
    }

    public InvalidFileNameFormatException(string fileName, string message) 
        : base(message)
    {
    }
}