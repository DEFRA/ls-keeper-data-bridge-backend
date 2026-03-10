namespace KeeperData.SamAPI.Customers
{
    public class FindCustomersRequest
    {
        public required IEnumerable<string> Ids { get; init; }
    }
}
