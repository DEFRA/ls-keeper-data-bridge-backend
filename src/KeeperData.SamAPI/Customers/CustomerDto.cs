namespace KeeperData.SamAPI.Customers
{
    public class CustomerDto
    {
        public required string Id { get; init; }
        public string? FirstName { get; init; }
        public string? LastName { get; init; }
    }
}