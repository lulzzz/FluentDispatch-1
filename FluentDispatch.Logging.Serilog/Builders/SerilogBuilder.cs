using System;
using FluentDispatch.Logging.Builders;
using FluentDispatch.Logging.Components;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Serilog.Core;

namespace FluentDispatch.Logging.Serilog.Builders
{
    /// <summary>
    /// Builder component which exposes a builder-pattern to construct a <see cref="ILoggerComponent"/>
    /// </summary>
    public sealed class SerilogBuilder : LoggerBuilder
    {
        private bool disposed;
        private readonly ILoggerFactory loggerFactory;
        private readonly IConfiguration configuration;
        private readonly LoggingLevelSwitch loggingLevelSwitch;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="loggerFactory"><see cref="ILoggerBuilder"/></param>
        /// <param name="configuration"><see cref="IConfiguration"/></param>
        /// <param name="loggingLevelSwitch"><see cref="loggingLevelSwitch"/></param>
        public SerilogBuilder(ILoggerFactory loggerFactory,
            IConfiguration configuration,
            LoggingLevelSwitch loggingLevelSwitch)
        {
            this.loggerFactory = loggerFactory;
            this.configuration = configuration;
            this.loggingLevelSwitch = loggingLevelSwitch;
        }

        /// <summary>
        /// Build the <see cref="ILoggerComponent"/> instance
        /// </summary>
        /// <returns><see cref="ILoggerComponent"/></returns>
        public override ILoggerComponent Build()
        {
            return new LoggerComponent(loggerFactory, configuration, loggingLevelSwitch);
        }

        /// <summary>
        /// Destructor
        /// </summary>
        ~SerilogBuilder()
        {
            Dispose(false);
        }

        /// <summary>
        /// Dispose resources
        /// </summary>
        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        private void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                loggerFactory?.Dispose();
            }

            disposed = true;
        }
    }
}