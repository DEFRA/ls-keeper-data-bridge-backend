namespace KeeperData.SamAPI.Locations
{
    public class Facility
    {
        public string Type { get; set; } = default!;
        public string Id { get; set; } = default!;
        public string? Name { get; set; }
        public string? FacilityType { get; set; }
        public string? BusinessActivity { get; set; }
    }
}