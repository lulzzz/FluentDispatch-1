﻿using System;
using System.Threading;
using System.Threading.Tasks;
using GrandCentralDispatch.Models;

namespace GrandCentralDispatch.Nodes.Remote.Async
{
    /// <summary>
    /// Node which process items.
    /// </summary>
    /// <typeparam name="TInput"></typeparam>
    /// <typeparam name="TOutput"></typeparam>
    internal interface IAsyncDispatcherQueueRemoteNode<TInput, TOutput> : IDisposable
    {
        /// <summary>
        /// Dispatch a <see cref="Func{TInput}"/> to the node.
        /// </summary>
        /// <typeparam name="TOutput"><see cref="TOutput"/></typeparam>
        /// <param name="item"><see cref="TInput"/></param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/></param>
        /// <returns><see cref="TOutput"/></returns>
        Task<TOutput> DispatchAsync(TInput item, CancellationToken cancellationToken);

        /// <summary>
        /// <see cref="NodeMetrics"/>
        /// </summary>
        NodeMetrics NodeMetrics { get; }
    }
}
