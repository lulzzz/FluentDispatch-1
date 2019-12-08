using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using FluentDispatch.Options;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;

namespace FluentDispatch.Processors.Direct
{
    internal abstract class DirectParallelProcessor<TInput> : DirectAbstractProcessor<TInput>
    {
        /// <summary>
        /// <see cref="DirectParallelProcessor{TInput}"/>
        /// </summary>
        /// <param name="circuitBreakerPolicy"><see cref="CircuitBreakerPolicy"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="progress">Progress of the current bulk</param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="logger"><see cref="ILogger"/></param>
        protected DirectParallelProcessor(AsyncCircuitBreakerPolicy circuitBreakerPolicy,
            ClusterOptions clusterOptions,
            IProgress<double> progress,
            CancellationTokenSource cts,
            ILogger logger) : base(circuitBreakerPolicy, clusterOptions, logger)
        {
             ItemsSubjectSubscription = SynchronizedItemsSubject
                .ObserveOn(new EventLoopScheduler(ts => new Thread(ts)
                    {IsBackground = true, Priority = ThreadPriority}))
                .Select(item =>
                {
                    return Observable.FromAsync(() =>
                    {
                        return CircuitBreakerPolicy.ExecuteAndCaptureAsync(
                            ct => Process(item, progress, ct), cts.Token);
                    });
                })
                // Dequeue in parallel
                .Merge()
                .Subscribe(unit =>
                    {
                        if (unit.Outcome == OutcomeType.Failure)
                        {
                            Logger.LogCritical(
                                unit.FinalException != null
                                    ? $"Could not process item: {unit.FinalException.Message}."
                                    : "An error has occured while processing the item.");
                        }
                    },
                    ex => Logger.LogError(ex.Message));

            ItemsExecutorSubjectSubscription = SynchronizedItemsExecutorSubject
                .ObserveOn(new EventLoopScheduler(ts => new Thread(ts)
                    {IsBackground = true, Priority = ThreadPriority}))
                .Select(item =>
                {
                    return Observable.FromAsync(() =>
                    {
                        return CircuitBreakerPolicy.ExecuteAndCaptureAsync(
                            ct => Process(item, progress, ct), cts.Token);
                    });
                })
                // Dequeue in parallel
                .Merge()
                .Subscribe(unit =>
                    {
                        if (unit.Outcome == OutcomeType.Failure)
                        {
                            Logger.LogCritical(
                                unit.FinalException != null
                                    ? $"Could not process item: {unit.FinalException.Message}."
                                    : "An error has occured while processing the item.");
                        }
                    },
                    ex => Logger.LogError(ex.Message));
        }
    }
}