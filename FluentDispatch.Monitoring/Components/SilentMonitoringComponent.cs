using App.Metrics.Builder;

namespace FluentDispatch.Monitoring.Components
{
    /// <summary>
    /// <inheritdoc cref="IMonitoringComponent"/>
    /// </summary>
    public class SilentMonitoringComponent : AbstractMonitoringComponent
    {
        public override void UseMonitoring(IMetricsReportingBuilder builder)
        {
        }
    }
}