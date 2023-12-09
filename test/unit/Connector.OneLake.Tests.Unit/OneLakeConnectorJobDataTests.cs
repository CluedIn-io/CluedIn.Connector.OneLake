using Newtonsoft.Json;
using Xunit;

namespace CluedIn.Connector.OneLake.Tests.Unit
{
    public class OneLakeConnectorJobDataTests
    {
        [Fact]
        public void OneLakeConnectorJobData_CouldBeDeserialized()
        {
            var configString = "{\"AccountName\":\"stdlrok1\",\"AccountKey\":\"***\",\"FileSystemName\":\"newfs\",\"DirectoryName\":\"dmytroTest\",\"ContainerName\":\"TargetContainer\",\"Configurations\":{\"AccountName\":\"stdlrok1\",\"AccountKey\":\"***\",\"FileSystemName\":\"newfs\",\"DirectoryName\":\"dmytroTest\",\"firstTime\":true},\"CrawlType\":0,\"TargetHost\":null,\"TargetCredentials\":null,\"TargetApiKey\":null,\"LastCrawlFinishTime\":\"0001-01-01T00:00:00+00:00\",\"LastestCursors\":null,\"IsFirstCrawl\":false,\"ExpectedTaskCount\":0,\"IgnoreNextCrawl\":false,\"ExpectedStatistics\":null,\"ExpectedTime\":\"00:00:00\",\"ExpectedData\":0,\"Errors\":null}";
            var jobData = JsonConvert.DeserializeObject<OneLakeConnectorJobData>(configString);

            Assert.NotNull(jobData.Configurations);
        }
    }
}
