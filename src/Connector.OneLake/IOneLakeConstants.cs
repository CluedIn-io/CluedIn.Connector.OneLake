
namespace CluedIn.Connector.OneLake
{
    public interface IOneLakeConstants : IConfigurationConstants
    {
        /// <summary>
        /// Environment key name for cache sync interval
        /// </summary>
        string CacheSyncIntervalKeyName { get; }

        /// <summary>
        /// Default value for Cache sync interval in milliseconds
        /// </summary>
        int CacheSyncIntervalDefaultValue { get; }

        /// <summary>
        /// Environment key name for cache records threshold
        /// </summary>
        string CacheRecordsThresholdKeyName { get; }

        /// <summary>
        /// Default value for Cache records threshold
        /// </summary>
        int CacheRecordsThresholdDefaultValue { get; }
    }
}
