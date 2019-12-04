using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using FluentDispatch.Host.Hosting;
using FluentDispatch.Logging.Serilog.Extensions;
using FluentDispatch.Monitoring.InfluxDb.Extensions;
using FluentDispatch.Monitoring.InfluxDb.Options;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace FluentDispatch.Cluster
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var host = FluentDispatchCluster<Startup>.CreateDefaultBuilder(
                    configureListeningPort: configuration => configuration.GetValue<int>("FLUENTDISPATCH_CLUSTER_LISTENING_PORT"),
                    loggerBuilder: (loggerBuilder, configuration) => loggerBuilder.UseSerilog(new LoggerConfiguration().ReadFrom
                        .Configuration(configuration)
                        .Enrich.FromLogContext()
                        .WriteTo.Console(), configuration),
                    (monitoringBuilder, configuration) =>
                        monitoringBuilder.UseInfluxDb(new InfluxDbMonitoringOptions(
                            new Uri(configuration.GetValue<string>("INFLUXDB_ENDPOINT")),
                            configuration.GetValue<string>("INFLUXDB_DATABASE"))).Build())
                .Build();
            await host.RunAsync();
        }
    }
}