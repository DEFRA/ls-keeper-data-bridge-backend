namespace KeeperData.Core.Messaging.Extensions;

public static class MessageAttributes
{
    private const string MESSAGE_SUFFIX = "Message";

    public static string ReplaceSuffix(this string? messageName)
    {
        return messageName?.EndsWith(MESSAGE_SUFFIX) == true
            ? messageName[..^MESSAGE_SUFFIX.Length]
            : messageName ?? string.Empty;
    }
}