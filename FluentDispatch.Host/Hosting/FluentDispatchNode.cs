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
        /// <param name="resolvers"></param>
        /// <returns>The initialized <see cref="IHostBuilder"/>.</returns>
        public static IHostBuilder CreateDefaultBuilder(
            params Type[] resolvers)
        {
            var builder = MagicOnionHost.CreateDefaultBuilder();
            builder.UseWindowsService();
            ConfigureHostConfigurationDefault(builder);
            ConfigureLoggingDefault(builder);
            var configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
            configurationBuilder.AddJsonFile(s =>
            {
                s.FileProvider = null;
                s.Path = "appsettings.json";
                s.Optional = false;
                s.ReloadOnChange = true;
                s.ResolveFileProvider();
            });
            configurationBuilder.AddEnvironmentVariables();
            var configuration = configurationBuilder.Build();
            builder.UseMagicOnion(
                new List<ServerPort>
                {
                    new ServerPort(IPAddress.Any.ToString(),
                        configuration.GetValue<int>("FLUENTDISPATCH_NODE_LISTENING_PORT"),
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
            builder.ConfigureServices(serviceCollection =>
            {
                var startup = (TStartup) Activator.CreateInstance(typeof(TStartup), configuration);
                startup.ConfigureServices(serviceCollection);
            });

            return builder;
        }

        private static void ConfigureHostConfigurationDefault(IHostBuilder builder)
        {
            builder.UseContentRoot(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
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