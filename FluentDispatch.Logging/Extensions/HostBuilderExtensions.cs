using System;
using FluentDispatch.Logging.Builders;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace FluentDispatch.Logging.Extensions
{
    /// <summary>
    /// Extension methods for <see cref="IHostBuilder"/>
    /// </summary>
    public static class HostBuilderExtensions
    {
        /// <summary>
        /// Use the configured <see cref="ILoggerBuilder"/> as provider
        /// </summary>
        /// <param name="builder"><see cref="IHostBuilder"/></param>
        /// <param name="configureLogger"><see cref="Func{TResult}"/></param>
        /// <returns><see cref="IHostBuilder"/></returns>
        public static IHostBuilder UseLogging(this IHostBuilder builder,
            Func<ILoggerBuilder, IConfiguration, ILoggerBuilder> configureLogger)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            IConfiguration configuration = null;
            builder
                .ConfigureAppConfiguration(config => { configuration = config.Build(); })
                .ConfigureServices(services => { services.AddLogging(configureLogger, configuration); });
            return builder;
        }
    }
}