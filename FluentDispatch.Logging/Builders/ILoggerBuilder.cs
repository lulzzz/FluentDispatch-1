using System;
using FluentDispatch.Logging.Components;

namespace FluentDispatch.Logging.Builders
{
    /// <summary>
    /// Builder component which exposes a builder-pattern to construct a <see cref="ILoggerComponent"/>
    /// </summary>
    public interface ILoggerBuilder : IDisposable
    {
        /// <summary>
        /// Build the <see cref="ILoggerComponent"/> instance
        /// </summary>
        /// <returns><see cref="ILoggerComponent"/></returns>
        ILoggerComponent Build();
    }
}