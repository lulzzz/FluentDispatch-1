using System.IO;
using System.Net;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using System;
using MagicOnion.Hosting;
using Grpc.Core;
using System.Collections.Generic;
using MagicOnion.Server;
using System.Linq;
using FluentDispatch.Resolvers;

namespace FluentDispatch.Host.Hosting
{
    public static class FluentDispatchNode<TStartup> where TStartup : NodeStartup
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IHostBuilder"/> class with pre-configured defaults.
        /// </summary>
        /// <param name="configureListeningPort">Listening port</param>
        /// <param name="resolvers">Resolvers</param>
        /// <returns>The initialized <see cref="IHostBuilder"/>.</returns>
        public static IHostBuilder CreateDefaultBuilder(
            Func<IConfiguration, int> configureListeningPort,
            params Type[] resolvers)
        {
            var builder = MagicOnionHost.CreateDefaultBuilder();
            ConfigureAppConfigurationDefault(builder);
            ConfigureServices(builder);
            ConfigureLoggingDefault(builder);
            var configurationBuilder = new ConfigurationBuilder();
            SetConfigurationBuilder(configurationBuilder);
            var configuration = configurationBuilder.Build();
            builder.UseMagicOnion(
                new List<ServerPort>
                {
                    new ServerPort(IPAddress.Any.ToString(),
                        configureListeningPort(configuration),
                        ServerCredentials.Insecure)
                },
                new MagicOnionOptions(true)
                {
                    MagicOnionLogger = new MagicOnionLogToGrpcLogger()
                },
                types: resolvers.Select(resolver =>
                {
                    var targetType = typeof(IResolver);
                    if (!targetType.GetTypeInfo().IsAssignableFrom(resolver.GetTypeInfo()))
                    {
                        throw new ArgumentException(nameof(resolver),
                            $"Type {resolver.Name} should implement IResolver interface.");
                    }

                    return resolver;
                }).Concat(new[]
                {
                    typeof(Hubs.Hub.NodeHub)
                }));

            return builder;
        }

        private static void ConfigureAppConfigurationDefault(IHostBuilder builder)
        {
            builder.ConfigureAppConfiguration((hostingContext, config) =>
            {
                SetConfigurationBuilder(config, hostingContext);
            });
        }

        private static void SetConfigurationBuilder(IConfigurationBuilder config)
        {
            config.SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
            config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            config.AddEnvironmentVariables();
        }

        private static void SetConfigurationBuilder(IConfigurationBuilder config, HostBuilderContext hostingContext)
        {
            config.SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
            config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            config.AddJsonFile(
                $"appsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json",
                optional: true, reloadOnChange: true);
            config.AddEnvironmentVariables();
        }

        private static void ConfigureServices(IHostBuilder builder)
        {
            builder.ConfigureServices((hostingContext, serviceCollection) =>
            {
                var startup = (TStartup) Activator.CreateInstance(typeof(TStartup), hostingContext.Configuration);
                startup.ConfigureServices(serviceCollection);
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