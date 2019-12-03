using Microsoft.Extensions.Logging;

namespace FluentDispatch.Logging.Components
{
    /// <summary>
    /// Component which wraps the logging underlying providers
    /// </summary>
    public interface ILoggerComponent : ILoggerProvider
    {
        /// <summary>
        /// Get the logging minimum level
        /// </summary>
        LogLevel GetMinimumLevel(ILogger logger);

        /// <summary>
        /// Get the logging minimum level
        /// </summary>
        LogLevel GetMinimumLevel<T>(ILogger<T> logger);

        /// <summary>
        /// Set the logging minimum level
        /// </summary>
        /// <param name="level"><see cref="LogLevel"/></param>
        void SetMinimumLevel(LogLevel level);

        /// <summary>
        /// Get a logger associated to a generic caller
        /// </summary>
        /// <returns><see cref="ILogger"/></returns>
        ILogger<T> CreateLogger<T>();

        /// <summary>
        /// Get an empty logger
        /// </summary>
        /// <returns><see cref="ILogger"/></returns>
        ILogger GetNullLogger();

        /// <summary>
        /// Get an empty logger
        /// </summary>
        /// <returns><see cref="ILogger"/></returns>
        ILogger GetNullLogger<T>();
    }
}