using System;
using FluentDispatch.Logging.Builders;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace FluentDispatch.Logging.Extensions
{
    /// <summary>
    /// Extensions methods for <see cref="IServiceCollection"/>
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Use the configured <see cref="ILoggerBuilder"/> as provider
        /// </summary>
        /// <param name="serviceCollection"><see cref="IServiceCollection"/></param>
        /// <param name="configureLogger"><see cref="Func{TResult}"/></param>
        /// <param name="configuration"><see cref="IConfiguration"/></param>
        /// <returns><see cref="IServiceCollection"/></returns>
        public static IServiceCollection AddLogging(this IServiceCollection serviceCollection,
            Func<ILoggerBuilder, IConfiguration, ILoggerBuilder> configureLogger,
            IConfiguration configuration)
        {
            serviceCollection.AddLogging(configure =>
            {
                var loggerBuilder = new LoggerBuilder();
                var loggerComponent = configureLogger(loggerBuilder, configuration).Build();
                serviceCollection.AddSingleton(loggerComponent);
                configure.ClearProviders();
                configure.AddProvider(loggerComponent);
            });
            return serviceCollection;
        }
    }
}
