using App.Metrics.AspNetCore;
using FluentDispatch.Monitoring.Components;
using Microsoft.AspNetCore.Hosting;

namespace FluentDispatch.Monitoring.Extensions
{
    public static class IWebHostBuilderExtensions
    {
        public static IWebHostBuilder UseMonitoring(this IWebHostBuilder builder, IMonitoringComponent monitoringComponent)
        {
            builder
                .ConfigureMetricsWithDefaults((context, bld) =>
                {
                    bld.Configuration.Configure(
                        options =>
                        {
                            options.Enabled = true;
                            options.ReportingEnabled = true;
                        });
                    monitoringComponent.UseMonitoring(bld.Report);
                });

            builder.UseMetricsWebTracking();
            builder.UseMetrics<MetricsStartup>();
            return builder;
        }
    }
}