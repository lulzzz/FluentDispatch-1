using FluentDispatch.Monitoring.Components;

namespace FluentDispatch.Monitoring.Builders
{
    /// <summary>
    /// Builder component which exposes a builder-pattern to construct a <see cref="IMonitoringComponent"/>
    /// </summary>
    public sealed class SilentMonitoringBuilder : MonitoringBuilder
    {
        /// <summary>
        /// Build the <see cref="IMonitoringComponent"/> instance
        /// </summary>
        /// <returns><see cref="IMonitoringComponent"/></returns>
        public override IMonitoringComponent Build()
        {
            return new SilentMonitoringComponent();
        }
    }
}
