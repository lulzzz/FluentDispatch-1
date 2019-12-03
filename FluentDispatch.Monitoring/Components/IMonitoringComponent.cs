using App.Metrics.Builder;

namespace FluentDispatch.Monitoring.Components
{
    /// <summary>
    /// Component which wraps the monitoring underlying providers
    /// </summary>
    public interface IMonitoringComponent
    {
        void UseMonitoring(IMetricsReportingBuilder builder);
    }
}