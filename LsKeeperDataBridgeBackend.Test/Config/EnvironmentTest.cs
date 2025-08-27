using Microsoft.AspNetCore.Builder;

namespace LsKeeperDataBridgeBackend.Test.Config;

public class EnvironmentTest
{

   [Fact]
   public void IsNotDevModeByDefault()
   { 
       var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions());
       var isDev = LsKeeperDataBridgeBackend.Config.Environment.IsDevMode(builder);
       Assert.False(isDev);
   }
}
