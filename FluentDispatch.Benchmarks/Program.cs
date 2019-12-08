using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.Options;
using FluentDispatch.Benchmarks.Models;
using FluentDispatch.Clusters;
using FluentDispatch.Options;

namespace FluentDispatch.Benchmarks
{
    [ShortRunJob]
    [Config(typeof(Config))]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [MemoryDiagnoser]
    public class ClusterBench
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                Add(CsvMeasurementsExporter.Default);
                Add(RPlotExporter.Default);
            }
        }

        private ICluster<Message> _parallelCluster;
        private Progress<double> _progress;

        [GlobalSetup]
        public void Setup()
        {
            _progress = new Progress<double>();
            _parallelCluster = new DirectCluster<Message>(
                new Resolver(),
                new OptionsWrapper<ClusterOptions>(new ClusterOptions(clusterSize: Environment.ProcessorCount,
                    nodeQueuingStrategy: NodeQueuingStrategy.Randomized,
                    clusterProcessingType: ClusterProcessingType.Parallel)),
                new OptionsWrapper<CircuitBreakerOptions>(new CircuitBreakerOptions()),
                _progress,
                new CancellationTokenSource());
        }

        [Benchmark(Description = "Fire-And-Forget FluentDispatch")]
        public async Task Dispatch_And_Process_In_Parallel()
        {
            var body = new ConcurrentBag<long>();
            const int target = 1_000;
            using var semaphore = new SemaphoreSlim(0);
            var messages = new ConcurrentBag<Message>();
            for (var i = 0; i < target; i++)
            {
                messages.Add(new Message(target, body, semaphore));
            }

            foreach (var message in messages)
            {
                _parallelCluster.Dispatch(message);
            }

            await semaphore.WaitAsync();
        }

        [Benchmark(Description = "Fire-And-Forget Task.Run")]
        public async Task Dispatch_TPL()
        {
            var body = new ConcurrentBag<long>();
            const int target = 1_000;
            using var semaphore = new SemaphoreSlim(0);
            var messages = new ConcurrentBag<Message>();
            for (var i = 0; i < target; i++)
            {
                messages.Add(new Message(target, body, semaphore));
            }

            foreach (var message in messages)
            {
#pragma warning disable 4014
                Task.Run(() =>
#pragma warning restore 4014
                {
                    message.Body.Add(Helper.FindPrimeNumber(10));
                    if (message.Body.Count == message.Target)
                        message.SemaphoreSlim.Release();

                    return message;
                });
            }

            await semaphore.WaitAsync();
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkRunner.Run<ClusterBench>();
        }
    }
}