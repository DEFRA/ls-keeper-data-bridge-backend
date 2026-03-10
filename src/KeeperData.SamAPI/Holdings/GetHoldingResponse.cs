namespace KeeperData.SamAPI.Holdings
{
    public class GetHoldingResponse
    {
        public HoldingData Data { get; set; } = default!;
        public Links Links { get; set; } = default!;
    }
}