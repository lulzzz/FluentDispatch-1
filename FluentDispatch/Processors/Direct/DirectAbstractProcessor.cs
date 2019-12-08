using System;
using System.Collections.Concurrent;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using FluentDispatch.Options;
using FluentDispatch.Processors.Unary;
using Microsoft.Extensions.Logging;
using Polly.CircuitBreaker;

namespace FluentDispatch.Processors.Direct
{
    internal abstract class DirectAbstractProcessor<TInput> : Processor, IUnaryProcessor<TInput>
    {
        protected readonly ISubject<TInput> SynchronizedItemsSubject;
        protected readonly ISubject<Func<TInput>> SynchronizedItemsExecutorSubject;

        protected readonly ConcurrentQueue<TInput> ItemsBuffer = new ConcurrentQueue<TInput>();

        protected readonly ConcurrentQueue<Func<TInput>> ItemsExecutorBuffer =
            new ConcurrentQueue<Func<TInput>>();

        protected IDisposable ItemsSubjectSubscription;
        protected IDisposable ItemsExecutorSubjectSubscription;

        protected DirectAbstractProcessor(AsyncCircuitBreakerPolicy circuitBreakerPolicy,
            ClusterOptions clusterOptions,
            ILogger logger) : base(circuitBreakerPolicy, clusterOptions, logger)
        {
            ISubject<TInput> itemsSubject = new Subject<TInput>();
            ISubject<Func<TInput>> itemsExecutorSubject = new Subject<Func<TInput>>();

            // SynchronizedItems are thread-safe objects in which we can push items concurrently
            SynchronizedItemsSubject = Subject.Synchronize(itemsSubject);
            SynchronizedItemsExecutorSubject = Subject.Synchronize(itemsExecutorSubject);
        }

        /// <summary>
        /// Push a new item to the queue.
        /// </summary>
        /// <param name="item"><see cref="TInput"/></param>
        /// <remarks>This call does not block the calling thread</remarks>
        public void Add(TInput item)
        {
            Interlocked.Increment(ref _totalItemsProcessed);
            SynchronizedItemsSubject.OnNext(item);
        }

        /// <summary>
        /// Push a new item to the queue.
        /// </summary>
        /// <param name="item"><see cref="TInput"/></param>
        /// <remarks>This call does not block the calling thread</remarks>
        public void Add(Func<TInput> item)
        {
            Interlocked.Increment(ref _totalItemsProcessed);
            SynchronizedItemsExecutorSubject.OnNext(item);
        }

        /// <summary>
        /// The bulk processor.
        /// </summary>
        /// <param name="item"><see cref="TInput"/> to process</param>
        /// <param name="progress">Progress of the current bulk</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/></param>
        /// <returns><see cref="Task"/></returns>
        protected abstract Task Process(TInput item, IProgress<double> progress,
            CancellationToken cancellationToken);

        /// <summary>
        /// The bulk processor.
        /// </summary>
        /// <param name="item"><see cref="TInput"/> to process</param>
        /// <param name="progress">Progress of the current bulk</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/></param>
        /// <returns><see cref="Task"/></returns>
        protected abstract Task Process(Func<TInput> item, IProgress<double> progress,
            CancellationToken cancellationToken);

        /// <summary>
        /// Indicates if current processor is full.
        /// </summary>
        /// <returns>True if full</returns>
        protected bool IsFull() => ItemsBuffer.Count + ItemsExecutorBuffer.Count >= ClusterOptions.NodeThrottling;

        /// <summary>
        /// Get current buffer size
        /// </summary>
        /// <returns>Buffer size</returns>
        protected int GetBufferSize() => ItemsBuffer.Count + ItemsExecutorBuffer.Count;

        /// <summary>
        /// Dispose timer
        /// </summary>
        /// <param name="disposing"></param>
        protected override void Dispose(bool disposing)
        {
            if (Disposed)
                return;

            if (disposing)
            {
                ItemsSubjectSubscription?.Dispose();
                ItemsExecutorSubjectSubscription?.Dispose();
            }

            Disposed = true;
            base.Dispose(disposing);
        }
    }
}