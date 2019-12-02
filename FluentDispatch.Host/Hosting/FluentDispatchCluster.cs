using System.IO;
using System.Reflection;
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
        /// <returns>The initialized <see cref="IHostBuilder"/>.</returns>
        public static IHostBuilder CreateDefaultBuilder()
        {
            var builder = new HostBuilder();
            builder.UseWindowsService();
            ConfigureHostConfigurationDefault(builder);
            ConfigureAppConfigurationDefault(builder);
            ConfigureLoggingDefault(builder);
            ConfigureWebDefaults(builder);
            return builder;
        }

        private static void ConfigureWebDefaults(IHostBuilder builder)
        {
            builder.ConfigureWebHostDefaults(webHostBuilder =>
            {
                webHostBuilder.UseKestrel((hostingContext, options) =>
                {
                    options.ListenAnyIP(hostingContext.Configuration.GetValue<int>(
                        "FLUENTDISPATCH_CLUSTER_LISTENING_PORT"));
                });
                webHostBuilder.UseMonitoring();
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
                var env = hostingContext.HostingEnvironment;
                config.AddJsonFile(s =>
                {
                    s.FileProvider = null;
                    s.Path = "appsettings.json";
                    s.Optional = false;
                    s.ReloadOnChange = true;
                    s.ResolveFileProvider();
                });
                config.AddJsonFile(s =>
                {
                    s.FileProvider = null;
                    s.Path = $"appsettings.{env.EnvironmentName}.json";
                    s.Optional = true;
                    s.ReloadOnChange = true;
                    s.ResolveFileProvider();
                });
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