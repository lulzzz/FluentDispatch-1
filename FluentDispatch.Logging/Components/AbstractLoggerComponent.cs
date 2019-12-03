using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FluentDispatch.Logging.Components
{
    /// <summary>
    /// <inheritdoc cref="ILoggerComponent"/>
    /// </summary>
    public abstract class AbstractLoggerComponent : ILoggerComponent
    {
        private bool disposed;
        private readonly ILoggerFactory loggerFactory;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        protected AbstractLoggerComponent(ILoggerFactory loggerFactory)
        {
            this.loggerFactory = loggerFactory;
        }

        /// <summary>
        /// <inheritdoc cref="GetMinimumLevel"/>
        /// </summary>
        public virtual LogLevel GetMinimumLevel(ILogger logger)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// <inheritdoc cref="GetMinimumLevel{T}"/>
        /// </summary>
        public virtual LogLevel GetMinimumLevel<T>(ILogger<T> logger)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// <inheritdoc cref="SetMinimumLevel"/>
        /// </summary>
        public virtual void SetMinimumLevel(LogLevel level)
        {
            throw new NotImplementedException();
        }

        public ILogger CreateLogger(string categoryName)
        {
            return loggerFactory.CreateLogger(categoryName);
        }

        /// <summary>
        /// <inheritdoc cref="CreateLogger{T}"/>
        /// </summary>
        public ILogger<T> CreateLogger<T>()
        {
            return loggerFactory.CreateLogger<T>();
        }

        /// <summary>
        /// <inheritdoc cref="GetNullLogger"/>
        /// </summary>
        public ILogger GetNullLogger()
        {
            return NullLogger.Instance;
        }

        /// <summary>
        /// <inheritdoc cref="GetNullLogger{T}"/>
        /// </summary>
        public ILogger GetNullLogger<T>()
        {
            return NullLogger<T>.Instance;
        }

        /// <summary>
        /// Destructor
        /// </summary>
        ~AbstractLoggerComponent()
        {
            Dispose(false);
        }

        /// <summary>
        /// <inheritdoc cref="Dispose"/>
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        
        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
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