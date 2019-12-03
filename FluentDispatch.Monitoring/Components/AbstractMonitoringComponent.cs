using System;
using App.Metrics.Builder;

namespace FluentDispatch.Monitoring.Components
{
    /// <summary>
    /// <inheritdoc cref="IMonitoringComponent"/>
    /// </summary>
    public abstract class AbstractMonitoringComponent : IMonitoringComponent
    {
        public virtual void UseMonitoring(IMetricsReportingBuilder builder)
        {
            throw new NotImplementedException(
                "No monitoring reporting has been specified, you must declare your monitoring intent by building against a provider as follow: UseInfluxDb() for instance.");
        }
    }
}