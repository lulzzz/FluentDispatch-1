using System;
using FluentDispatch.Monitoring.Options;

namespace FluentDispatch.Monitoring.InfluxDb.Options
{
    /// <summary>
    /// Monitoring options for InfluxDb
    /// </summary>
    public class InfluxDbMonitoringOptions : MonitoringOptions
    {
        /// <summary>
        /// <see cref="InfluxDbMonitoringOptions"/>
        /// </summary>
        /// <param name="baseUri">Gets or sets the base URI of the InfluxDB API.</param>
        /// <param name="database">Gets or sets the InfluxDB database name used to report metrics.</param>
        /// <param name="createDatabaseIfNotExists">Gets or sets a value indicating whether or not to attempt to create the specified database if it does not exist</param>
        /// <param name="flushIntervalInMilliseconds">Gets or sets the flush metrics interval</param>
        /// <param name="backoffPeriodInMilliseconds"></param>
        /// <param name="failuresBeforeBackoff"></param>
        /// <param name="timeoutInMilliseconds"></param>
        public InfluxDbMonitoringOptions(Uri baseUri,
            string database,
            bool createDatabaseIfNotExists = true,
            int flushIntervalInMilliseconds = 5000,
            int backoffPeriodInMilliseconds = 30_000,
            int failuresBeforeBackoff = 3,
            int timeoutInMilliseconds = 5000)
        {
            BaseUri = baseUri;
            Database = database;
            CreateDataBaseIfNotExists = createDatabaseIfNotExists;
            FlushInterval = TimeSpan.FromMilliseconds(flushIntervalInMilliseconds);
            BackoffPeriod = TimeSpan.FromMilliseconds(backoffPeriodInMilliseconds);
            FailuresBeforeBackoff = failuresBeforeBackoff;
            Timeout = TimeSpan.FromMilliseconds(timeoutInMilliseconds);
        }

        /// <summary>
        /// Gets or sets the base URI of the InfluxDB API.
        /// </summary>
        public Uri BaseUri { get; set; }

        /// <summary>
        /// Gets or sets the InfluxDB database name used to report metrics.
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether or not to attempt to create the specified database if it does not exist
        /// </summary>
        public bool CreateDataBaseIfNotExists { get; set; } = true;

        /// <summary>
        /// Gets or sets the flush metrics interval
        /// </summary>
        public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(5);

        public TimeSpan BackoffPeriod { get; set; } = TimeSpan.FromSeconds(30);
        
        public int FailuresBeforeBackoff { get; set; } = 3;
        
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);
    }
}