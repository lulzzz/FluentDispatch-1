using App.Metrics;
using App.Metrics.Builder;
using FluentDispatch.Monitoring.Components;
using FluentDispatch.Monitoring.InfluxDb.Options;

namespace FluentDispatch.Monitoring.InfluxDb
{
    /// <summary>
    /// <inheritdoc cref="IMonitoringComponent"/>
    /// </summary>
    public class MonitoringComponent : AbstractMonitoringComponent
    {
        /// <summary>
        /// <see cref="InfluxDbMonitoringOptions"/>
        /// </summary>
        private readonly InfluxDbMonitoringOptions _influxDbMonitoringOptions;

        public MonitoringComponent(InfluxDbMonitoringOptions influxDbMonitoringOptions)
        {
            _influxDbMonitoringOptions = influxDbMonitoringOptions;
        }

        public override void UseMonitoring(IMetricsReportingBuilder builder)
        {
            builder.ToInfluxDb(options =>
            {
                options.InfluxDb.BaseUri = _influxDbMonitoringOptions.BaseUri;
                options.InfluxDb.Database = _influxDbMonitoringOptions.Database;
                options.InfluxDb.CreateDataBaseIfNotExists = _influxDbMonitoringOptions.CreateDataBaseIfNotExists;
                options.FlushInterval = _influxDbMonitoringOptions.FlushInterval;
                options.HttpPolicy.Timeout = _influxDbMonitoringOptions.Timeout;
                options.HttpPolicy.FailuresBeforeBackoff = _influxDbMonitoringOptions.FailuresBeforeBackoff;
                options.HttpPolicy.BackoffPeriod = _influxDbMonitoringOptions.BackoffPeriod;
            });
        }
    }
}