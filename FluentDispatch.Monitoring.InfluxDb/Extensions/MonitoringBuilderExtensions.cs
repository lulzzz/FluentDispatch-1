using FluentDispatch.Monitoring.Builders;
using FluentDispatch.Monitoring.InfluxDb.Builders;
using FluentDispatch.Monitoring.InfluxDb.Options;

namespace FluentDispatch.Monitoring.InfluxDb.Extensions
{
    /// <summary>
    /// Convenient extension methods to build a <see cref="IMonitoringBuilder"/>
    /// </summary>
    public static class MonitoringBuilderExtensions
    {
        /// <summary>
        /// Build with Json
        /// </summary>
        /// <param name="builder"><see cref="IMonitoringBuilder"/></param>
        /// <param name="influxDbMonitoringOptions"><see cref="InfluxDbMonitoringOptions"/></param>
        /// <returns><see cref="IMonitoringBuilder"/></returns>
        public static IMonitoringBuilder UseInfluxDb(this IMonitoringBuilder builder,
            InfluxDbMonitoringOptions influxDbMonitoringOptions)
        {
            return new InfluxDbBuilder(influxDbMonitoringOptions);
        }
    }
}
