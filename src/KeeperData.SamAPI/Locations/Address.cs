namespace KeeperData.SamAPI.Locations
{
    public class Address
    {
        public AddressableObject PrimaryAddressableObject { get; set; } = default!;
        public AddressableObject SecondaryAddressableObject { get; set; } = default!;
        public string? Street { get; set; }
        public string? Locality { get; set; }
        public string? Town { get; set; }
        public string? Postcode { get; set; }
        public string? CountryCode { get; set; }
    }
}