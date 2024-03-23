using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using System;
using System.IO;
using System.Threading.Tasks;

namespace CluedIn.Connector.OneLake.Connector
{
    public class OneLakeClient : IOneLakeClient
    {
        public async Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(OneLakeConnectorJobData configuration)
        {
            var dataLakeFileSystemClient = await EnsureDataLakeFileSystemClientAsync(configuration);
            string uploadFolder = configuration.ItemName + "." + configuration.ItemType + "/" + configuration.ItemFolder + "/"; //"jlalakehouse.Lakehouse/Files/"

            var directoryClient = dataLakeFileSystemClient.GetDirectoryClient(uploadFolder);
            if (!await directoryClient.ExistsAsync())
            {
                directoryClient = await dataLakeFileSystemClient.CreateDirectoryAsync(uploadFolder);
            }

            return directoryClient;
        }

        public async Task SaveData(OneLakeConnectorJobData configuration, string content, string fileName)
        {
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            var options = new DataLakeFileUploadOptions
            {
                HttpHeaders = new PathHttpHeaders { ContentType = "application/json" }
            };

            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
            var response = await dataLakeFileClient.UploadAsync(stream, options);
            
            if (response?.Value == null)
            {
                throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.UploadAsync)} did not return a valid path");
            }
        }

        public async Task DeleteFile(OneLakeConnectorJobData configuration, string fileName)
        {
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);
            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            var response = await dataLakeFileClient.DeleteAsync();

            if (response.Status != 200)
            {
                throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.DeleteAsync)} returned {response.Status}");
            }
        }

        private DataLakeServiceClient GetDataLakeServiceClient(OneLakeConnectorJobData configuration)
        {
            {

                string accountName = "onelake";

                ClientSecretCredential sharedKeyCredential =
        new ClientSecretCredential(configuration.TenantId, configuration.ClientId, configuration.ClientSecret);

                string dfsUri = $"https://{accountName}.dfs.fabric.microsoft.com";

                DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(
                    new Uri(dfsUri),
                    sharedKeyCredential);

                return dataLakeServiceClient;
            }
        }

        private async Task<DataLakeFileSystemClient> EnsureDataLakeFileSystemClientAsync(
            OneLakeConnectorJobData configuration)
        {
             var dataLakeServiceClient = GetDataLakeServiceClient(configuration);
            var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(configuration.WorkspaceName);
            if (!await dataLakeFileSystemClient.ExistsAsync())
            {
                dataLakeFileSystemClient = await dataLakeServiceClient.CreateFileSystemAsync(configuration.WorkspaceName);
            }

            return dataLakeFileSystemClient;
        }
    }
}
