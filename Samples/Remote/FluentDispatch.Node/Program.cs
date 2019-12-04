using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using FluentDispatch.Host.Hosting;
using FluentDispatch.Logging.Serilog.Extensions;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace FluentDispatch.Node
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var host = FluentDispatchNode<Startup>.CreateDefaultBuilder(
                    configureListeningPort: configuration => configuration.GetValue<int>("FLUENTDISPATCH_NODE_LISTENING_PORT"),
                    loggerBuilder: (loggerBuilder, configuration) => loggerBuilder.UseSerilog(new LoggerConfiguration().ReadFrom
                        .Configuration(configuration)
                        .Enrich.FromLogContext()
                        .WriteTo.Console(), configuration),
                    typeof(Contract.Resolvers.MetadataResolver),
                    typeof(Contract.Resolvers.SentimentPredictionResolver),
                    typeof(Contract.Resolvers.IndexerResolver))
                .Build();
            await host.RunAsync();
        }
    }
}