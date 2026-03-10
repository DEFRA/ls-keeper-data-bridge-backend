namespace KeeperData.SamAPI.Holdings
{
    public class FindHoldingsResponse
    {
        public List<HoldingData> Data { get; set; } = [];
        public Links Links { get; set; } = default!;
    }
}