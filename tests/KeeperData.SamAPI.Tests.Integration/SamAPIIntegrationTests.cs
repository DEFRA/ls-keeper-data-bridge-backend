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
            var result = await fixture.SamApi.FindCustomers(new[] { "C123456" });

            testOutputHelper.WriteLine($"Found {result?.Data.Count} customers.");
            testOutputHelper.WriteLine($"First record has firstname:{result?.Data.First().FirstName} and lastname:{result?.Data.First().LastName}");
        }

        [Fact]
        public async Task FindHoldings_ReturnsData()
        {
            var result = await fixture.SamApi.FindHoldings(["08/139/0167"], 1, 5);

            testOutputHelper.WriteLine($"Found {result?.Data.Count} holdings.");
            testOutputHelper.WriteLine($"First record has type:{result?.Data.First().Type} and cphType:{result?.Data.First().CphType}");
        }

        [Fact]
        public async Task FindLocations_ReturnsData()
        {
            var response = await fixture.SamApi.FindLocations(new[] { "L123", "L456", "L789" });

            var firstLocation = response?.Data.FirstOrDefault();
            
            testOutputHelper.WriteLine($"Found {response?.Data.Count} locations");
            testOutputHelper.WriteLine($"First record has location type: {firstLocation?.Type} and osMapReference {firstLocation?.OsMapReference}");
        }
    }
}