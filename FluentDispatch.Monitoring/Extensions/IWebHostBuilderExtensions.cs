using System;
using App.Metrics;
using App.Metrics.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;

namespace FluentDispatch.Monitoring.Extensions
{
    public static class IWebHostBuilderExtensions
    {
        public static IWebHostBuilder UseMonitoring(this IWebHostBuilder builder)
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
                    if (!string.IsNullOrEmpty(context.Configuration.GetValue<string>("INFLUXDB_ENDPOINT")))
                    {
                        bld.Report.ToInfluxDb(options =>
                        {
                            options.InfluxDb.BaseUri = new Uri(context.Configuration.GetValue<string>("INFLUXDB_ENDPOINT"));
                            options.InfluxDb.Database = "fluentdispatch";
                            options.FlushInterval = TimeSpan.FromSeconds(5);
                            options.InfluxDb.CreateDataBaseIfNotExists = true;
                            options.HttpPolicy.Timeout = TimeSpan.FromSeconds(10);
                        });
                    }
                });

            builder.UseMetricsWebTracking();
            builder.UseMetrics<MetricsStartup>();
            return builder;
        }
    }
}