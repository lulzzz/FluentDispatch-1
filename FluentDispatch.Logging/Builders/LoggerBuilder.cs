using System;
using FluentDispatch.Logging.Components;

namespace FluentDispatch.Logging.Builders
{
    /// <summary>
    /// <inheritdoc cref="ILoggerBuilder"/>
    /// </summary>
    public class LoggerBuilder : ILoggerBuilder
    {
        /// <summary>
        /// <inheritdoc cref="Build"/>
        /// </summary>
        public virtual ILoggerComponent Build()
        {
            throw new NotImplementedException(
                "No logger was involved, declare your intent by calling UseSerilog().");
        }

        /// <summary>
        /// <inheritdoc cref="Dispose"/>
        /// </summary>
        public virtual void Dispose()
        {

        }
    }
}