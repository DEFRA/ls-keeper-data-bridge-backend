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

        [Fact]
        public async Task GetHoldings_ReturnsData()
        {
            var result = await samApi.GetHoldingAsync("15", "270", "1919");

            Assert.NotNull(result);
        }

        [Fact]
        public async Task FindHoldings_ReturnsData()
        {
            var result = await samApi.FindHoldingsAsync(["15/270/1919"], 1, 5);

            Assert.NotNull(result);
        }
    }
}