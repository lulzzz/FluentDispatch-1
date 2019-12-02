using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using FluentDispatch.Host.Hosting;

namespace FluentDispatch.Cluster
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var host = FluentDispatchCluster<Startup>.CreateDefaultBuilder()
                .Build();
            await host.RunAsync();
        }
    }
}