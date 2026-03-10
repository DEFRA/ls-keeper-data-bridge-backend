namespace KeeperData.SamAPI.Tests.Integration
{
    public class SamApiIntegrationTests : IClassFixture<SamApiFixture>
    {
        private readonly ISamApi samApi;

        public SamApiIntegrationTests(SamApiFixture fixture)
        {
            samApi = fixture.SamApi;
        }

        [Fact]
        public async Task FindCustomers_ReturnsData()
        {
            var result = await samApi.FindCustomersAsync(new[] { "C123456" });

            Assert.NotNull(result);
        }
    }
}