using Azure.Storage.Files.DataLake;
using System.Threading.Tasks;

namespace CluedIn.Connector.OneLake.Connector
{
    public interface IOneLakeClient
    {
        Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(OneLakeConnectorJobData configuration);
        Task SaveData(OneLakeConnectorJobData configuration, string content, string fileName);
        Task DeleteFile(OneLakeConnectorJobData configuration, string fileName);
    }
}
