using System;
using FluentDispatch.Logging.Components;
using FluentDispatch.Logging.Serilog.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Serilog.Core;
using Serilog.Events;

namespace FluentDispatch.Logging.Serilog
{
    /// <summary>
    /// Serilog logger component
    /// </summary>
    public sealed class LoggerComponent : AbstractLoggerComponent
    {
        private readonly IDisposable tokenDisposable;
        private readonly IConfiguration configuration;
        private readonly LoggingLevelSwitch loggingLevelSwitch;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        /// <param name="configuration"><see cref="IConfiguration"/></param>
        /// <param name="loggingLevelSwitch"><see cref="LoggingLevelSwitch"/></param>
        public LoggerComponent(ILoggerFactory loggerFactory,
            IConfiguration configuration,
            LoggingLevelSwitch loggingLevelSwitch) : base(loggerFactory)
        {
            this.configuration = configuration;
            this.loggingLevelSwitch = loggingLevelSwitch;
            tokenDisposable = ChangeToken.OnChange(
                configuration.GetReloadToken,
                () => { ApplyMinimumLevel("Logging:LogLevel:Default"); });
        }

        /// <summary>
        /// Apply the minimum logging level
        /// </summary>
        private void ApplyMinimumLevel(string loggingLevel)
        {
            var level = ParseLogEventLevel(configuration[loggingLevel]).ToLogLevel();
            SetMinimumLevel(level);
        }

        /// <summary>
        /// Parse the Serilog logging string level
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private static LogEventLevel ParseLogEventLevel(string value)
        {
            if (!Enum.TryParse(value, out LogEventLevel parsedLevel))
                throw new InvalidOperationException($"The value {value} is not a valid Serilog level.");
            return parsedLevel;
        }

        /// <summary>
        /// Get the logging minimum level
        /// </summary>
        public override LogLevel GetMinimumLevel(ILogger logger)
        {
            foreach (LogLevel level in Enum.GetValues(typeof(LogLevel)))
            {
                if (logger.IsEnabled(level))
                {
                    return level;
                }
            }

            throw new Exception($"Could not retrieve minimum log level from {nameof(logger)}.");
        }

        /// <summary>
        /// Get the logging minimum level
        /// </summary>
        public override LogLevel GetMinimumLevel<T>(ILogger<T> logger)
        {
            foreach (LogLevel level in Enum.GetValues(typeof(LogLevel)))
            {
                if (logger.IsEnabled(level))
                {
                    return level;
                }
            }

            throw new Exception($"Could not retrieve minimum log level from {nameof(logger)}.");
        }

        /// <summary>
        /// Set the logging minimum level
        /// </summary>
        /// <param name="level"><see cref="LogLevel"/></param>
        public override void SetMinimumLevel(LogLevel level)
        {
            loggingLevelSwitch.MinimumLevel = level.ToLogLevelEvent();
        }

        /// <summary>
        /// <inheritdoc cref="Dispose"/>
        /// </summary>
        /// <param name="disposing"></param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            tokenDisposable?.Dispose();
        }
    }
}