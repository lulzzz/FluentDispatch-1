using System;
using Microsoft.Extensions.Logging;
using Serilog.Events;

namespace FluentDispatch.Logging.Serilog.Extensions
{
    public static class LogLevelExtensions
    {
        public static LogLevel ToLogLevel(this LogEventLevel level)
        {
            switch (level)
            {
                case LogEventLevel.Fatal:
                    return LogLevel.Critical;
                case LogEventLevel.Error:
                    return LogLevel.Error;
                case LogEventLevel.Warning:
                    return LogLevel.Warning;
                case LogEventLevel.Information:
                    return LogLevel.Information;
                case LogEventLevel.Debug:
                    return LogLevel.Debug;
                case LogEventLevel.Verbose:
                    return LogLevel.Trace;
                default:
                    throw new NotImplementedException($"{nameof(level)} mapping is not implemented.");
            }
        }

        public static LogEventLevel ToLogLevelEvent(this LogLevel level)
        {
            switch (level)
            {
                case LogLevel.Critical:
                    return LogEventLevel.Fatal;
                case LogLevel.Error:
                    return LogEventLevel.Error;
                case LogLevel.Warning:
                    return LogEventLevel.Warning;
                case LogLevel.Information:
                    return LogEventLevel.Information;
                case LogLevel.Debug:
                    return LogEventLevel.Debug;
                case LogLevel.Trace:
                    return LogEventLevel.Verbose;
                default:
                    throw new NotImplementedException($"{nameof(level)} mapping is not implemented.");
            }
        }
    }
}