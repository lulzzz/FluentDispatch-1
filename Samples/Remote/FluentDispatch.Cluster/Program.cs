using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using FluentDispatch.Host.Hosting;
using FluentDispatch.Monitoring.InfluxDb.Extensions;
using FluentDispatch.Monitoring.InfluxDb.Options;
using Microsoft.Extensions.Configuration;

namespace FluentDispatch.Cluster
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var host = FluentDispatchCluster<Startup>.CreateDefaultBuilder(
                    configuration => configuration.GetValue<int>("FLUENTDISPATCH_CLUSTER_LISTENING_PORT"),
                    (monitoringBuilder, configuration) =>
                        monitoringBuilder.UseInfluxDb(new InfluxDbMonitoringOptions(
                            new Uri(configuration.GetValue<string>("INFLUXDB_ENDPOINT")),
                            configuration.GetValue<string>("INFLUXDB_DATABASE"))).Build())
                .Build();
            await host.RunAsync();
        }
    }
}