using System;
using FluentDispatch.Monitoring.Components;

namespace FluentDispatch.Monitoring.Builders
{
    /// <summary>
    /// <inheritdoc cref="IMonitoringBuilder"/>
    /// </summary>
    public class MonitoringBuilder : IMonitoringBuilder
    {
        /// <summary>
        /// <inheritdoc cref="Build"/>
        /// </summary>
        public virtual IMonitoringComponent Build()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// <inheritdoc cref="Dispose"/>
        /// </summary>
        public virtual void Dispose()
        {

        }
    }
}
