namespace KeeperData.SamAPI.Security
{
    public interface ITokenClient
    {
        Task<string> GetAccessTokenAsync(CancellationToken ct = default);
    }
}