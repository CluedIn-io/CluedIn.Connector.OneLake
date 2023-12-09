using CluedIn.Core.Connectors;

namespace CluedIn.Connector.OneLake.Connector
{
    public class OneLakeConnectorContainer : IConnectorContainer
    {
        public string Name { get; set; }
        public string Id { get; set; }
        public string FullyQualifiedName { get; set; }
    }
}
