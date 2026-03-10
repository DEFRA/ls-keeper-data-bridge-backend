namespace KeeperData.SamAPI.Locations
{
    public class LivestockUnit
    {
        public string Type { get; set; } = default!;
        public string Id { get; set; } = default!;
        public int AnimalQuantities { get; set; }
        public string? Species { get; set; }
    }
}