namespace KeeperData.SamAPI.Holdings
{
    public class HoldingData
    {
        public string Type { get; set; } = default!;
        public string Id { get; set; } = default!;
        public string CphType { get; set; } = default!;
        public HoldingRelationships Relationships { get; set; } = default!;
    }
}