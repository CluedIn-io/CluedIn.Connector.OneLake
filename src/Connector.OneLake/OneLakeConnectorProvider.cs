using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.OneLake
{
    public class OneLakeConnectorProvider : ConnectorProviderBase<OneLakeConnectorProvider>
    {
        public OneLakeConnectorProvider([NotNull] ApplicationContext appContext,
            IOneLakeConstants configuration, ILogger<OneLakeConnectorProvider> logger)
            : base(appContext, configuration, logger)
        {
        }

        protected override IEnumerable<string> ProviderNameParts => new[]
        {
           OneLakeConstants.WorkspaceName,
           OneLakeConstants.ItemFolder,
           OneLakeConstants.ItemType,
           OneLakeConstants.ItemName,
           OneLakeConstants.ClientId,
           OneLakeConstants.ClientSecret,
           OneLakeConstants.TenantId
        };
    }
}
