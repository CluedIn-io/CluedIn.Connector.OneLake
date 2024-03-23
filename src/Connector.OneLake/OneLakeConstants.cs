using CluedIn.Core.Providers;
using System;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.OneLake
{
    public class OneLakeConstants : ConfigurationConstantsBase, IOneLakeConstants
    {
        public const string WorkspaceName = nameof(WorkspaceName);
        public const string ItemName = nameof(ItemName);
        public const string ItemType = nameof(ItemType);
        public const string ItemFolder = nameof(ItemFolder);
        public const string ClientId = nameof(ClientId);
        public const string ClientSecret = nameof(ClientSecret);
        public const string TenantId = nameof(TenantId);

        public OneLakeConstants() : base(Guid.Parse("36C1B087-97C0-4460-A813-6E4EA1D1BC9A"),
            providerName: "OneLake Connector",
            componentName: "OneLakeConnector",
            icon: "Resources.onelake.svg",
            domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
            about: "Supports publishing of data to OneLake.",
            authMethods: OneLakeAuthMethods,
            guideDetails: "Supports publishing of data to OneLake.",
            guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
        {
        }

        /// <summary>
        /// Environment key name for cache sync interval
        /// </summary>
        public string CacheSyncIntervalKeyName => "Streams.OneLakeConnector.CacheSyncInterval";

        /// <summary>
        /// Default value for Cache sync interval in milliseconds
        /// </summary>
        public int CacheSyncIntervalDefaultValue => 60_000;

        /// <summary>
        /// Environment key name for cache records threshold
        /// </summary>
        public string CacheRecordsThresholdKeyName => "Streams.OneLakeConnector.CacheRecordsThreshold";

        /// <summary>
        /// Default value for Cache records threshold
        /// </summary>
        public int CacheRecordsThresholdDefaultValue => 50;

        private static AuthMethods OneLakeAuthMethods => new AuthMethods
        {
            token = new[]
            {
                new Control
                {
                    name = WorkspaceName,
                    displayName = WorkspaceName,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = ItemName,
                    displayName = ItemName,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = ItemType,
                    displayName = ItemType,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = ItemFolder,
                    displayName = ItemFolder,
                    type = "input",
                    isRequired = true
                }
                ,
                new Control
                {
                    name = ClientId,
                    displayName = ClientId,
                    type = "input",
                    isRequired = true
                }
                ,
                new Control
                {
                    name = ClientSecret,
                    displayName = ClientSecret,
                    type = "password",
                    isRequired = true
                }
                ,
                new Control
                {
                    name = TenantId,
                    displayName = TenantId,
                    type = "input",
                    isRequired = true
                }
            }
        };
    }
}
