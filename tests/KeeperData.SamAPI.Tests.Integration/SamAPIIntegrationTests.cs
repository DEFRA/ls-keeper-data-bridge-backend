using Xunit.Abstractions;

namespace KeeperData.SamAPI.Tests.Integration
{
    public class SamApiIntegrationTests : IClassFixture<SamApiFixture>
    {
        private readonly SamApiFixture fixture;
        private readonly ITestOutputHelper testOutputHelper;

        public SamApiIntegrationTests(SamApiFixture fixture, ITestOutputHelper testOutputHelper)
        {
            this.fixture = fixture;
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task FindCustomers_ReturnsData()
        {
            var result = await fixture.SamApi.FindCustomersAsync(new[] { "C123456" });

            testOutputHelper.WriteLine($"Found {result?.Data.Count} customers.");
            testOutputHelper.WriteLine($"{result?.Data.First().FirstName} {result?.Data.First().LastName}");
        }

        [Fact]
        public async Task GetHoldings_ReturnsData()
        {
            var result = await fixture.SamApi.GetHoldingAsync("15", "270", "1919");

            testOutputHelper.WriteLine($"{result?.Data.Id}, {result?.Data.CphType}");
        }

        [Fact]
        public async Task FindHoldings_ReturnsData()
        {
            var result = await fixture.SamApi.FindHoldingsAsync(["15/270/1919"], 1, 5);

            testOutputHelper.WriteLine($"Found {result?.Data.Count} holdings.");
        }

        [Fact]
        public async Task FindLocations_ReturnsData()
        {
            var response = await fixture.SamApi.FindLocationsAsync(new[] { "L123", "L456", "L789" });

            var firstLocation = response?.Data.FirstOrDefault();
            
            testOutputHelper.WriteLine($"Found {response?.Data.Count} locations. First location type: {firstLocation?.Type} and osMapReference {firstLocation?.OsMapReference}");
        }
    }
}