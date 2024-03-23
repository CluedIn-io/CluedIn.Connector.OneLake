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
        public string WorkspaceName => Configurations[OneLakeConstants.WorkspaceName] as string;
        public string ItemName => Configurations[OneLakeConstants.ItemName] as string;
        public string ItemType => Configurations[OneLakeConstants.ItemType] as string;
        public string ItemFolder => Configurations[OneLakeConstants.ItemFolder] as string;
        public string ClientId => Configurations[OneLakeConstants.ClientId] as string;
        public string ClientSecret => Configurations[OneLakeConstants.ClientSecret] as string;
        public string TenantId => Configurations[OneLakeConstants.TenantId] as string;

        public string ContainerName { get; }

        public override int GetHashCode()
        {
            return HashCode.Combine(WorkspaceName, ItemName, ItemType, ItemFolder, ClientId, ClientSecret, TenantId);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as OneLakeConnectorJobData);
        }

        public bool Equals(OneLakeConnectorJobData other)
        {
            return other != null &&
                WorkspaceName == other.WorkspaceName &&
                ItemName == other.ItemName &&
                ItemType == other.ItemType &&
                ItemFolder == other.ItemFolder &&
                ClientId == other.ClientId &&
                ClientSecret == other.ClientSecret &&
                TenantId == other.TenantId &&
                ContainerName == other.ContainerName;
                
        }
    }
}
