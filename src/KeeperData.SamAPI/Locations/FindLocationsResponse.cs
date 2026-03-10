using KeeperData.SamAPI.Holdings;

namespace KeeperData.SamAPI.Locations
{
    public class FindLocationsResponse
    {
        public List<LocationData> Data { get; set; } = [];
        public Links Links { get; set; } = default!;
    }
}
