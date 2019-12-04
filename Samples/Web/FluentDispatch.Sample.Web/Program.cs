using System.Threading.Tasks;
using FluentDispatch.Host.Hosting;
using FluentDispatch.Logging.Serilog.Extensions;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace FluentDispatch.Sample.Web
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            using var host = FluentDispatchCluster<Startup>
                .CreateDefaultBuilder(configureListeningPort: configuration => 5000,
                    loggerBuilder: (loggerBuilder, configuration) => loggerBuilder.UseSerilog(new LoggerConfiguration().ReadFrom
                        .Configuration(configuration)
                        .Enrich.FromLogContext()
                        .WriteTo.Console(), configuration))
                .Build();
            await host.RunAsync();
        }
    }
}