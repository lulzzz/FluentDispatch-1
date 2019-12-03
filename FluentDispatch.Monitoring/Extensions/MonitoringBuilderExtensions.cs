using FluentDispatch.Monitoring.Builders;

namespace FluentDispatch.Monitoring.Extensions
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
        /// <returns><see cref="IMonitoringBuilder"/></returns>
        public static IMonitoringBuilder UseSilent(this IMonitoringBuilder builder)
        {
            return new SilentMonitoringBuilder();
        }
    }
}