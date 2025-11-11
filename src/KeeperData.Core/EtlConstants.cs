namespace KeeperData.Core;

public static class EtlConstants
{
    public const int DefaultLookbackDays = 200;
    public const string DatePattern = "yyyyMMdd";
    public const string DateTimePattern = "yyyyMMddHHmmss";
    public const string CompositeKeyDelimiter = "@@";
    public const string LineageEventIdDelimiter = "||";

    // S3-compliant metadata keys (lowercase with x-amz-meta- prefix)
    // S3 automatically prefixes user metadata with "x-amz-meta-" and lowercases the keys
    public const string MetadataKeySourceEncryptedLength = "x-amz-meta-sourceencryptedlength";
    public const string MetadataKeySourceETag = "x-amz-meta-sourceetag";
}