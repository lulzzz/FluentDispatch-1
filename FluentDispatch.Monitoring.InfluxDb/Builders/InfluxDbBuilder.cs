using FluentDispatch.Monitoring.Builders;
using FluentDispatch.Monitoring.Components;
using FluentDispatch.Monitoring.InfluxDb.Options;

namespace FluentDispatch.Monitoring.InfluxDb.Builders
{
    /// <summary>
    /// Builder component which exposes a builder-pattern to construct a <see cref="IMonitoringComponent"/>
    /// </summary>
    public sealed class InfluxDbBuilder : MonitoringBuilder
    {
        /// <summary>
        /// <see cref="InfluxDbMonitoringOptions"/>
        /// </summary>
        private readonly InfluxDbMonitoringOptions _influxDbMonitoringOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="influxDbMonitoringOptions"><see cref="InfluxDbMonitoringOptions"/></param>
        public InfluxDbBuilder(InfluxDbMonitoringOptions influxDbMonitoringOptions)
        {
            this._influxDbMonitoringOptions = influxDbMonitoringOptions;
        }

        /// <summary>
        /// Build the <see cref="IMonitoringComponent"/> instance
        /// </summary>
        /// <returns><see cref="IMonitoringComponent"/></returns>
        public override IMonitoringComponent Build()
        {
            return new MonitoringComponent(_influxDbMonitoringOptions);
        }
    }
}