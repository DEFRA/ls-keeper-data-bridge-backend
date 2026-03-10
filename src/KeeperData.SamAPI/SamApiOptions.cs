namespace KeeperData.SamAPI
{
    public sealed class SamApiOptions
    {
        public const string SectionName = "SamApi";

        public required string BaseUrl { get; init; }
        public required string ClientId { get; init; }
        public required string ClientSecret { get; init; }
    }
}