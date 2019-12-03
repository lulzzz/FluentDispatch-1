using FluentDispatch.Logging.Builders;
using FluentDispatch.Logging.Serilog.Builders;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;

namespace FluentDispatch.Logging.Serilog.Extensions
{
    /// <summary>
    /// Convenient extension methods to build a <see cref="ILoggerBuilder"/>
    /// </summary>
    public static class LoggerBuilderExtensions
    {
        /// <summary>
        /// Build with Serilog using existing Serilog configuration
        /// </summary>
        /// <param name="builder"><see cref="LoggerBuilder"/></param>
        /// <param name="loggerConfiguration"><see cref="LoggerConfiguration"/></param>
        /// <param name="configuration"><see cref="IConfiguration"/></param>
        /// <returns><see cref="ILoggerBuilder"/></returns>
        public static ILoggerBuilder UseSerilog(this ILoggerBuilder builder,
            LoggerConfiguration loggerConfiguration,
            IConfiguration configuration)
        {
            var levelSwitch = new LoggingLevelSwitch();
            var loggerFactory = new LoggerFactory();
            var config = loggerConfiguration.MinimumLevel.ControlledBy(levelSwitch);
            loggerFactory.AddSerilog(config.CreateLogger(), dispose: true);

            return new SerilogBuilder(loggerFactory, configuration, levelSwitch);
        }

        /// <summary>
        /// Build with Serilog
        /// </summary>
        /// <param name="builder"><see cref="LoggerBuilder"/></param>
        /// <param name="configuration"><see cref="IConfiguration"/></param>
        /// <returns><see cref="ILoggerBuilder"/></returns>
        public static ILoggerBuilder UseSerilog(this ILoggerBuilder builder,
            IConfiguration configuration)
        {
            var levelSwitch = new LoggingLevelSwitch();
            var loggerFactory = new LoggerFactory();
            var config = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .MinimumLevel.ControlledBy(levelSwitch);
            loggerFactory.AddSerilog(config.CreateLogger(), dispose: true);

            return new SerilogBuilder(loggerFactory, configuration, levelSwitch);
        }
    }
}