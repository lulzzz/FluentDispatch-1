using System;
using System.IO;
using System.Reflection;
using FluentDispatch.Monitoring.Builders;
using FluentDispatch.Monitoring.Components;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using FluentDispatch.Monitoring.Extensions;

namespace FluentDispatch.Host.Hosting
{
    public static class FluentDispatchCluster<TStartup> where TStartup : ClusterStartup
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IHostBuilder"/> class with pre-configured defaults.
        /// </summary>
        /// <param name="configureListeningPort">Configure the listening port</param>
        /// <param name="monitoringBuilder">Monitoring builder</param>
        /// <returns>The initialized <see cref="IHostBuilder"/>.</returns>
        public static IHostBuilder CreateDefaultBuilder(
            Func<IConfiguration, int> configureListeningPort,
            Func<IMonitoringBuilder, IConfiguration, IMonitoringComponent> monitoringBuilder = null)
        {
            var hostBuilder = new HostBuilder();
            ConfigureHostConfigurationDefault(hostBuilder);
            ConfigureAppConfigurationDefault(hostBuilder);
            ConfigureLoggingDefault(hostBuilder);
            ConfigureWebDefaults(hostBuilder, configureListeningPort, monitoringBuilder);
            return hostBuilder;
        }

        private static void ConfigureWebDefaults(IHostBuilder hostBuilder,
            Func<IConfiguration, int> configureListeningPort,
            Func<IMonitoringBuilder, IConfiguration, IMonitoringComponent> monitoringBuilder = null)
        {
            hostBuilder.ConfigureWebHostDefaults(webHostBuilder =>
            {
                webHostBuilder.UseKestrel((hostingContext, options) =>
                {
                    options.ListenAnyIP(configureListeningPort(hostingContext.Configuration));
                });

                if (monitoringBuilder != null)
                {
                    webHostBuilder.ConfigureAppConfiguration(configurationBuilder =>
                    {
                        var configuration = configurationBuilder.Build();
                        webHostBuilder.UseMonitoring(monitoringBuilder(new MonitoringBuilder(),
                            configuration));
                    });
                }
                else
                {
                    webHostBuilder.UseMonitoring(new MonitoringBuilder().UseSilent().Build());
                }

                webHostBuilder.UseStartup<TStartup>();
            });
        }

        private static void ConfigureHostConfigurationDefault(IHostBuilder builder)
        {
            builder.UseContentRoot(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
        }

        private static void ConfigureAppConfigurationDefault(IHostBuilder builder)
        {
            builder.ConfigureAppConfiguration((hostingContext, config) =>
            {
                config.SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
                config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
                config.AddJsonFile(
                    $"appsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json",
                    optional: true, reloadOnChange: true);
                config.AddEnvironmentVariables();
            });
        }

        private static void ConfigureLoggingDefault(IHostBuilder builder)
        {
            builder.UseSerilog((hostingContext, loggerConfiguration) => loggerConfiguration
                .ReadFrom.Configuration(hostingContext.Configuration)
                .Enrich.FromLogContext()
                .WriteTo.Console()
            );
        }
    }
}