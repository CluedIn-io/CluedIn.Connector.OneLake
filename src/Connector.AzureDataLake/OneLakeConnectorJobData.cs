using System;
using System.Collections.Generic;

namespace CluedIn.Connector.OneLake
{
    public class OneLakeConnectorJobData : CrawlJobDataWrapper
    {
        public OneLakeConnectorJobData(IDictionary<string, object> configurations, string containerName = null) : base(configurations)
        {
            ContainerName = containerName;
        }

        public string AccountName => Configurations[OneLakeConstants.AccountName] as string;
        public string AccountKey => Configurations[OneLakeConstants.AccountKey] as string;
        public string FileSystemName => Configurations[OneLakeConstants.FileSystemName] as string;
        public string DirectoryName => Configurations[OneLakeConstants.DirectoryName] as string;
        public string ContainerName { get; }

        public override int GetHashCode()
        {
            return HashCode.Combine(AccountName, AccountKey, FileSystemName, DirectoryName, ContainerName);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as OneLakeConnectorJobData);
        }

        public bool Equals(OneLakeConnectorJobData other)
        {
            return other != null &&
                AccountName == other.AccountName &&
                AccountKey == other.AccountKey &&
                FileSystemName == other.FileSystemName &&
                DirectoryName == other.DirectoryName &&
                ContainerName == other.ContainerName;
                
        }
    }
}
