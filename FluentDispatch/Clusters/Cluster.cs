using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using FluentDispatch.Exceptions;
using FluentDispatch.Models;
using FluentDispatch.Nodes;
using FluentDispatch.Options;
using FluentDispatch.Resolvers;
using Polly;
using FluentDispatch.Nodes.Local.Async;
using FluentDispatch.Nodes.Local.Direct;
using FluentDispatch.Nodes.Remote.Async;
using FluentDispatch.Nodes.Local.Unary;
using FluentDispatch.Nodes.Remote.Unary;
using FluentDispatch.Nodes.Local.Dual;
using FluentDispatch.Nodes.Remote.Direct;
using FluentDispatch.Nodes.Remote.Dual;

namespace FluentDispatch.Clusters
{
    /// <summary>
    /// The cluster which is in charge of distributing the load to the configured nodes.
    /// </summary>
    /// <typeparam name="TInput"></typeparam>
    /// <typeparam name="TOutput"></typeparam>
    public class AsyncCluster<TInput, TOutput> : ClusterBase, IAsyncCluster<TInput, TOutput>
    {
        /// <summary>
        /// Local atomic nodes of the cluster, which process items immediately
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IAsyncDispatcherLocalNode<TInput, TOutput>> _localAtomicNodes =
            new ConcurrentDictionary<Guid, IAsyncDispatcherLocalNode<TInput, TOutput>>();

        /// <summary>
        /// Local queue nodes of the cluster, which process items using a dispatcher queue
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IAsyncDispatcherQueueLocalNode<TInput, TOutput>> _localQueueNodes =
            new ConcurrentDictionary<Guid, IAsyncDispatcherQueueLocalNode<TInput, TOutput>>();

        /// <summary>
        /// Remote atomic nodes of the cluster, which process items immediately
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IAsyncDispatcherRemoteNode<TInput, TOutput>> _remoteAtomicNodes =
            new ConcurrentDictionary<Guid, IAsyncDispatcherRemoteNode<TInput, TOutput>>();

        /// <summary>
        /// Remote queue nodes of the cluster, which process items using a dispatcher queue
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IAsyncDispatcherQueueRemoteNode<TInput, TOutput>> _remoteQueueNodes
            =
            new ConcurrentDictionary<Guid, IAsyncDispatcherQueueRemoteNode<TInput, TOutput>>();

        /// <summary>
        /// Node health subscriptions
        /// </summary>
        private readonly ConcurrentBag<IDisposable> _nodeHealthSubscriptions = new ConcurrentBag<IDisposable>();

        /// <summary>
        /// <see cref="CircuitBreakerOptions"/>
        /// </summary>
        private readonly CircuitBreakerOptions _circuitBreakerOptions;

        /// <summary>
        /// <see cref="AsyncCluster{TInput,TOutput}"/>
        /// </summary>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        public AsyncCluster(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CircuitBreakerOptions> circuitBreakerOptions,
            CancellationTokenSource cts = null,
            ILoggerFactory loggerFactory = null) :
            this(cts, clusterOptions.Value, circuitBreakerOptions.Value,
                loggerFactory)
        {

        }

        /// <summary>
        /// <see cref="AsyncCluster{TInput,TOutput}"/>
        /// </summary>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        private AsyncCluster(
            CancellationTokenSource cts,
            ClusterOptions clusterOptions,
            CircuitBreakerOptions circuitBreakerOptions,
            ILoggerFactory loggerFactory) : base(null, cts, clusterOptions,
            loggerFactory == null
                ? NullLogger<AsyncCluster<TInput, TOutput>>.Instance
                : loggerFactory.CreateLogger<AsyncCluster<TInput, TOutput>>(), loggerFactory)
        {
            try
            {
                _circuitBreakerOptions = circuitBreakerOptions;
                if (ClusterOptions.ExecuteRemotely)
                {
                    if (!ClusterOptions.Hosts.Any())
                    {
                        throw new FluentDispatchException(
                            "Hosts must be provided.");
                    }

                    foreach (var host in ClusterOptions.Hosts)
                    {
                        var remoteAtomicNode = (IAsyncDispatcherRemoteNode<TInput, TOutput>) Activator.CreateInstance(
                            typeof(AsyncDispatcherRemoteNode<TInput, TOutput>),
                            host,
                            CancellationTokenSource,
                            circuitBreakerOptions,
                            clusterOptions,
                            Logger);
                        _remoteAtomicNodes.TryAdd(remoteAtomicNode.NodeMetrics.Id, remoteAtomicNode);

                        if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                        {
                            var remoteQueueNode =
                                (IAsyncDispatcherQueueRemoteNode<TInput, TOutput>) Activator.CreateInstance(
                                    typeof(AsyncParallelDispatcherRemoteNode<TInput, TOutput>),
                                    host,
                                    Progress,
                                    CancellationTokenSource,
                                    circuitBreakerOptions,
                                    clusterOptions,
                                    Logger);
                            _remoteQueueNodes.TryAdd(remoteQueueNode.NodeMetrics.Id, remoteQueueNode);
                        }
                        else if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                        {
                            var remoteQueueNode =
                                (IAsyncDispatcherQueueRemoteNode<TInput, TOutput>) Activator.CreateInstance(
                                    typeof(AsyncSequentialDispatcherRemoteNode<TInput, TOutput>),
                                    host,
                                    Progress,
                                    CancellationTokenSource,
                                    circuitBreakerOptions,
                                    clusterOptions,
                                    Logger);
                            _remoteQueueNodes.TryAdd(remoteQueueNode.NodeMetrics.Id, remoteQueueNode);
                        }
                        else
                        {
                            throw new NotImplementedException(
                                $"{nameof(ClusterProcessingType)} of value {clusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                        }
                    }
                }
                else
                {
                    for (var i = 0; i < clusterOptions.ClusterSize; i++)
                    {
                        var localAtomicNode = (IAsyncDispatcherLocalNode<TInput, TOutput>) Activator.CreateInstance(
                            typeof(AsyncDispatcherLocalNode<TInput, TOutput>),
                            CancellationTokenSource,
                            circuitBreakerOptions,
                            clusterOptions,
                            Logger);
                        _localAtomicNodes.TryAdd(localAtomicNode.NodeMetrics.Id, localAtomicNode);

                        if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                        {
                            var localQueueNode =
                                (IAsyncDispatcherQueueLocalNode<TInput, TOutput>) Activator.CreateInstance(
                                    typeof(AsyncParallelDispatcherLocalNode<TInput, TOutput>),
                                    Progress,
                                    CancellationTokenSource,
                                    circuitBreakerOptions,
                                    clusterOptions,
                                    Logger);
                            _localQueueNodes.TryAdd(localQueueNode.NodeMetrics.Id, localQueueNode);
                        }
                        else if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                        {
                            var localQueueNode =
                                (IAsyncDispatcherQueueLocalNode<TInput, TOutput>) Activator.CreateInstance(
                                    typeof(AsyncSequentialDispatcherLocalNode<TInput, TOutput>),
                                    Progress,
                                    CancellationTokenSource,
                                    circuitBreakerOptions,
                                    clusterOptions,
                                    Logger);
                            _localQueueNodes.TryAdd(localQueueNode.NodeMetrics.Id, localQueueNode);
                        }
                        else
                        {
                            throw new NotImplementedException(
                                $"{nameof(ClusterProcessingType)} of value {clusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                        }
                    }
                }

                foreach (var node in _localAtomicNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeLocalAtomicNodeHealth));
                }

                foreach (var node in _localQueueNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeLocalQueueNodeHealth));
                }

                foreach (var node in _remoteAtomicNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeRemoteAtomicNodeHealth));
                }

                foreach (var node in _remoteQueueNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeRemoteQueueNodeHealth));
                }

                LogClusterOptions(circuitBreakerOptions,
                    _localAtomicNodes.Count + _localQueueNodes.Count + _remoteAtomicNodes.Count +
                    _remoteQueueNodes.Count);
                Logger.LogInformation("Cluster successfully initialized.");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex.Message);
            }
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeLocalAtomicNodeHealth(Guid guid)
        {
            if (_localAtomicNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeRemoteAtomicNodeHealth(Guid guid)
        {
            if (_remoteAtomicNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeLocalQueueNodeHealth(Guid guid)
        {
            if (_localQueueNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeRemoteQueueNodeHealth(Guid guid)
        {
            if (_remoteQueueNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute cluster health
        /// </summary>
        protected override void ComputeClusterHealth()
        {
            ClusterMetrics.CurrentThroughput = _localAtomicNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput) +
                                               _localQueueNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput) +
                                               _remoteAtomicNodes.Sum(node =>
                                                   node.Value.NodeMetrics.CurrentThroughput) +
                                               _remoteQueueNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput);
            base.ComputeClusterHealth();
        }

        /// <summary>
        /// Execute an item against the cluster immediately
        /// </summary>
        /// <typeparam name="TOutput"><see cref="TOutput"/></typeparam>
        /// <param name="selector"><see cref="Func{TResult}"/></param>
        /// <param name="item"><see cref="TInput"/></param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/></param>
        /// <returns><see cref="TOutput"/></returns>
        public async Task<TOutput> ExecuteAsync(Func<TInput, Task<TOutput>> selector, TInput item,
            CancellationToken cancellationToken)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localAtomicNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localAtomicNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        return await node.Value.ExecuteAsync(selector, item, cancellationToken);
                    }
                    else
                    {
                        var node = _localAtomicNodes.ElementAt(Random.Value.Next(0,
                            _localAtomicNodes.Count));
                        return await node.Value.ExecuteAsync(selector, item, cancellationToken);
                    }
                }
                else
                {
                    const string message = "There is no available node for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                const string message = "Could not dispatch item, this method cannot be called in a remote context.";
                Logger.LogError(message);
                throw new FluentDispatchException(message);
            }
        }

        /// <summary>
        /// Dispatch an item to the cluster using a dispatcher queue (using the Window parameter from <see cref="ClusterOptions"/>)
        /// </summary>
        /// <typeparam name="TOutput"><see cref="TOutput"/></typeparam>
        /// <param name="selector"><see cref="Func{TResult}"/></param>
        /// <param name="item"><see cref="TInput"/></param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/></param>
        /// <returns><see cref="TOutput"/></returns>
        public async Task<TOutput> DispatchAsync(Func<TInput, Task<TOutput>> selector, TInput item,
            CancellationToken cancellationToken)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localQueueNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localQueueNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        return await node.Value.DispatchAsync(selector, item, cancellationToken);
                    }
                    else
                    {
                        var node = _localQueueNodes.ElementAt(Random.Value.Next(0,
                            _localQueueNodes.Count));
                        return await node.Value.DispatchAsync(selector, item, cancellationToken);
                    }
                }
                else
                {
                    const string message = "There is no available node for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                const string message = "Could not dispatch item, this method cannot be called in a remote context.";
                Logger.LogError(message);
                throw new FluentDispatchException(message);
            }
        }

        /// <summary>
        /// Execute an item against the cluster immediately
        /// </summary>
        /// <typeparam name="TOutput"><see cref="TOutput"/></typeparam>
        /// <param name="item"><see cref="TInput"/></param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/></param>
        /// <returns><see cref="TOutput"/></returns>
        public async Task<TOutput> ExecuteAsync(TInput item, CancellationToken cancellationToken)
        {
            if (ClusterOptions.ExecuteRemotely)
            {
                var availableNodes = _remoteAtomicNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        return await node.Value.ExecuteAsync(item, cancellationToken);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        return await node.Value.ExecuteAsync(item, cancellationToken);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        return await node.Value.ExecuteAsync(item, cancellationToken);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteQueueNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                        throw new FluentDispatchException(message);
                    }
                }
            }
            else
            {
                const string message = "Could not dispatch item, this method cannot be called in a local context.";
                Logger.LogError(message);
                throw new FluentDispatchException(message);
            }
        }

        /// <summary>
        /// Dispatch an item to the cluster using a dispatcher queue (using the Window parameter from <see cref="ClusterOptions"/>)
        /// </summary>
        /// <typeparam name="TOutput"><see cref="TOutput"/></typeparam>
        /// <param name="item"><see cref="TInput"/></param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/></param>
        /// <returns><see cref="TOutput"/></returns>
        public async Task<TOutput> DispatchAsync(TInput item, CancellationToken cancellationToken)
        {
            if (ClusterOptions.ExecuteRemotely)
            {
                var availableNodes = _remoteQueueNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        return await node.Value.DispatchAsync(item, cancellationToken);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        return await node.Value.DispatchAsync(item, cancellationToken);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        return await node.Value.DispatchAsync(item, cancellationToken);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteQueueNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                        throw new FluentDispatchException(message);
                    }
                }
            }
            else
            {
                const string message = "Could not dispatch item, this method cannot be called in a local context.";
                Logger.LogError(message);
                throw new FluentDispatchException(message);
            }
        }

        /// <summary>
        /// Get nodes from the cluster
        /// </summary>
        public IReadOnlyCollection<INode> Nodes
        {
            get
            {
                var localAtomicNodes = _localAtomicNodes.Select(node => node.Value as INode);
                var localQueueNodes = _localQueueNodes.Select(node => node.Value as INode);
                var remoteAtomicNodes = _remoteAtomicNodes.Select(node => node.Value as INode);
                var remoteQueueNodes = _remoteQueueNodes.Select(node => node.Value as INode);
                return new ReadOnlyCollection<INode>(localAtomicNodes.Concat(localQueueNodes).Concat(remoteAtomicNodes)
                    .Concat(remoteQueueNodes).ToList());

            }
        }

        /// <summary>
        /// Add a node
        /// </summary>
        /// <param name="host">Specified host if the node is a remote node</param>
        public void AddNode(Host host = null)
        {
            if (ClusterOptions.ExecuteRemotely)
            {
                if (host == null)
                {
                    throw new FluentDispatchException(
                        "Host must be provided.");
                }

                var remoteAtomicNode = (IAsyncDispatcherRemoteNode<TInput, TOutput>) Activator.CreateInstance(
                    typeof(AsyncDispatcherRemoteNode<TInput, TOutput>),
                    host,
                    CancellationTokenSource,
                    _circuitBreakerOptions,
                    ClusterOptions,
                    Logger);
                _remoteAtomicNodes.TryAdd(remoteAtomicNode.NodeMetrics.Id, remoteAtomicNode);

                if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                {
                    var remoteQueueNode = (IAsyncDispatcherQueueRemoteNode<TInput, TOutput>) Activator.CreateInstance(
                        typeof(AsyncParallelDispatcherRemoteNode<TInput, TOutput>),
                        host,
                        Progress,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _remoteQueueNodes.TryAdd(remoteQueueNode.NodeMetrics.Id, remoteQueueNode);
                }
                else if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                {
                    var remoteQueueNode =
                        (IAsyncDispatcherQueueRemoteNode<TInput, TOutput>) Activator.CreateInstance(
                            typeof(AsyncSequentialDispatcherRemoteNode<TInput, TOutput>),
                            host,
                            Progress,
                            CancellationTokenSource,
                            _circuitBreakerOptions,
                            ClusterOptions,
                            Logger);
                    _remoteQueueNodes.TryAdd(remoteQueueNode.NodeMetrics.Id, remoteQueueNode);
                }
                else
                {
                    throw new NotImplementedException(
                        $"{nameof(ClusterProcessingType)} of value {ClusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                }
            }
            else
            {
                var loalAtomicNode = (IAsyncDispatcherLocalNode<TInput, TOutput>) Activator.CreateInstance(
                    typeof(AsyncDispatcherLocalNode<TInput, TOutput>),
                    CancellationTokenSource,
                    _circuitBreakerOptions,
                    ClusterOptions,
                    Logger);
                _localAtomicNodes.TryAdd(loalAtomicNode.NodeMetrics.Id, loalAtomicNode);

                if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                {
                    var localQueueNode =
                        (IAsyncDispatcherQueueLocalNode<TInput, TOutput>) Activator.CreateInstance(
                            typeof(AsyncParallelDispatcherLocalNode<TInput, TOutput>),
                            Progress,
                            CancellationTokenSource,
                            _circuitBreakerOptions,
                            ClusterOptions,
                            Logger);
                    _localQueueNodes.TryAdd(localQueueNode.NodeMetrics.Id, localQueueNode);
                }
                else if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                {
                    var localQueueNode =
                        (IAsyncDispatcherQueueLocalNode<TInput, TOutput>) Activator.CreateInstance(
                            typeof(AsyncSequentialDispatcherLocalNode<TInput, TOutput>),
                            Progress,
                            CancellationTokenSource,
                            _circuitBreakerOptions,
                            ClusterOptions,
                            Logger);
                    _localQueueNodes.TryAdd(localQueueNode.NodeMetrics.Id, localQueueNode);
                }
                else
                {
                    throw new NotImplementedException(
                        $"{nameof(ClusterProcessingType)} of value {ClusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                }
            }

            _nodeHealthSubscriptions.Add(_localAtomicNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeLocalAtomicNodeHealth));
            _nodeHealthSubscriptions.Add(_localQueueNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeLocalQueueNodeHealth));
            _nodeHealthSubscriptions.Add(_remoteAtomicNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeRemoteAtomicNodeHealth));
            _nodeHealthSubscriptions.Add(_remoteQueueNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeRemoteQueueNodeHealth));
        }

        /// <summary>
        /// Delete a node
        /// </summary>
        /// <param name="nodeId">Node Id</param>
        public void DeleteNode(Guid nodeId)
        {
            _localAtomicNodes.TryRemove(nodeId, out _);
            _localQueueNodes.TryRemove(nodeId, out _);
            _remoteAtomicNodes.TryRemove(nodeId, out _);
            _remoteQueueNodes.TryRemove(nodeId, out _);
        }

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
                foreach (var node in _localAtomicNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var node in _localQueueNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var node in _remoteAtomicNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var node in _remoteQueueNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var nodeHealthSubscription in _nodeHealthSubscriptions)
                {
                    nodeHealthSubscription?.Dispose();
                }
            }

            Disposed = true;
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// The cluster which is in charge of distributing the load to the configured nodes.
    /// </summary>
    /// <typeparam name="TInput"></typeparam>
    public class Cluster<TInput> : ClusterBase, ICluster<TInput>
    {
        /// <summary>
        /// Local nodes of the cluster
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IUnaryDispatcherLocalNode<TInput>> _localNodes =
            new ConcurrentDictionary<Guid, IUnaryDispatcherLocalNode<TInput>>();

        /// <summary>
        /// Remote nodes of the cluster
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IUnaryDispatcherRemoteNode<TInput>> _remoteNodes =
            new ConcurrentDictionary<Guid, IUnaryDispatcherRemoteNode<TInput>>();

        /// <summary>
        /// Node health subscriptions
        /// </summary>
        private readonly ConcurrentBag<IDisposable> _nodeHealthSubscriptions = new ConcurrentBag<IDisposable>();

        /// <summary>
        /// <see cref="CircuitBreakerOptions"/>
        /// </summary>
        private readonly CircuitBreakerOptions _circuitBreakerOptions;

        /// <summary>
        /// <see cref="FuncResolver{TInput}"/>
        /// </summary>
        private readonly FuncResolver<TInput> _funcResolver;

        /// <summary>
        /// <see cref="Cluster{TInput}"/>
        /// </summary>
        /// <param name="resolver"><see cref="Resolver{TInput}"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="progress"><see cref="Progress{TInput}"/></param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        public Cluster(
            FuncResolver<TInput> resolver,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CircuitBreakerOptions> circuitBreakerOptions,
            IProgress<double> progress = null,
            CancellationTokenSource cts = null,
            ILoggerFactory loggerFactory = null) :
            this(resolver, progress, cts, clusterOptions.Value, circuitBreakerOptions.Value,
                loggerFactory)
        {

        }

        /// <summary>
        /// <see cref="Cluster{TInput}"/>
        /// </summary>
        /// <param name="funcResolver">Resolve the action to execute asynchronously for each items when they are dequeued.</param>
        /// <param name="progress">Progress of the current bulk</param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        private Cluster(
            FuncResolver<TInput> funcResolver,
            IProgress<double> progress,
            CancellationTokenSource cts,
            ClusterOptions clusterOptions,
            CircuitBreakerOptions circuitBreakerOptions,
            ILoggerFactory loggerFactory) : base(progress, cts, clusterOptions,
            loggerFactory == null
                ? NullLogger<Cluster<TInput>>.Instance
                : loggerFactory.CreateLogger<Cluster<TInput>>(), loggerFactory)
        {
            try
            {
                _funcResolver = funcResolver;
                _circuitBreakerOptions = circuitBreakerOptions;
                if (ClusterOptions.ExecuteRemotely)
                {
                    if (!ClusterOptions.Hosts.Any())
                    {
                        throw new FluentDispatchException(
                            "Hosts must be provided.");
                    }

                    foreach (var host in ClusterOptions.Hosts)
                    {
                        if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                        {
                            var remoteNode = (IUnaryDispatcherRemoteNode<TInput>) Activator.CreateInstance(
                                typeof(UnaryParallelDispatcherRemoteNode<TInput>),
                                PersistentCache,
                                Progress,
                                host,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                        }
                        else if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                        {
                            var remoteNode = (IUnaryDispatcherRemoteNode<TInput>) Activator.CreateInstance(
                                typeof(UnarySequentialDispatcherRemoteNode<TInput>),
                                PersistentCache,
                                Progress,
                                host,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                        }
                        else
                        {
                            throw new NotImplementedException(
                                $"{nameof(ClusterProcessingType)} of value {clusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                        }
                    }
                }
                else
                {
                    for (var i = 0; i < clusterOptions.ClusterSize; i++)
                    {
                        if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                        {
                            var localNode = (IUnaryDispatcherLocalNode<TInput>) Activator.CreateInstance(
                                typeof(UnaryParallelDispatcherLocalNode<TInput>),
                                PersistentCache,
                                funcResolver.GetItemFunc(),
                                Progress,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                        }
                        else if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                        {
                            var localNode = (IUnaryDispatcherLocalNode<TInput>) Activator.CreateInstance(
                                typeof(UnarySequentialDispatcherLocalNode<TInput>),
                                PersistentCache,
                                funcResolver.GetItemFunc(),
                                Progress,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                        }
                        else
                        {
                            throw new NotImplementedException(
                                $"{nameof(ClusterProcessingType)} of value {clusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                        }
                    }
                }

                foreach (var node in _localNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeLocalNodeHealth));
                }

                foreach (var node in _remoteNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeRemoteNodeHealth));
                }

                LogClusterOptions(circuitBreakerOptions, _localNodes.Count + _remoteNodes.Count);
                var policyResult = Policy.Handle<Exception>().RetryAsync().ExecuteAndCaptureAsync(async () =>
                {
                    var persistedItems =
                        (await PersistentCache.CacheProvider.RetrieveItemsAsync<TInput>()).ToList();
                    await PersistentCache.CacheProvider.FlushDatabaseAsync();
                    if (persistedItems.Any())
                    {
                        Logger.LogInformation(
                            "Cluster was shutdown while items remained to be processed. Submitting...");
                        foreach (var item in persistedItems)
                        {
                            Dispatch(item);
                        }

                        Logger.LogInformation("Remaining items have been successfully processed.");
                    }
                }).GetAwaiter().GetResult();
                if (policyResult.Outcome == OutcomeType.Failure)
                {
                    Logger.LogError(
                        $"Error while processing previously failed items: {policyResult.FinalException?.Message ?? string.Empty}.");
                }

                Logger.LogInformation("Cluster successfully initialized.");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex.Message);
            }
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeLocalNodeHealth(Guid guid)
        {
            if (_localNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeRemoteNodeHealth(Guid guid)
        {
            if (_remoteNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute cluster health
        /// </summary>
        protected override void ComputeClusterHealth()
        {
            ClusterMetrics.CurrentThroughput = _localNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput) +
                                               _remoteNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput);
            base.ComputeClusterHealth();
        }

        /// <summary>
        /// Dispatch an item to the cluster, to be processed by the configured nodes.
        /// </summary>
        /// <remarks>
        /// This won't block the calling thread and this won't never throw any exception.
        /// A retry and circuit breaker policies will gracefully handle non successful attempts.
        /// </remarks>
        /// <param name="item">The item to process</param>
        public void Dispatch(TInput item)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        node.Value.Dispatch(item);
                    }
                    else
                    {
                        var node = _localNodes.ElementAt(Random.Value.Next(0,
                            _localNodes.Count));
                        node.Value.Dispatch(item);
                    }
                }
                else
                {
                    const string message = "There is no node available for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                var availableNodes = _remoteNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        node.Value.Dispatch(item);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        node.Value.Dispatch(item);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        node.Value.Dispatch(item);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                    }
                }
            }
        }

        /// <summary>
        /// Dispatch an item to the cluster, to be processed by the configured nodes.
        /// </summary>
        /// <remarks>
        /// This won't block the calling thread and this won't never throw any exception.
        /// A retry and circuit breaker policies will gracefully handle non successful attempts.
        /// </remarks>
        /// <param name="itemProducer">The item producer to process</param>
        public void Dispatch(Func<TInput> itemProducer)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        node.Value.Dispatch(itemProducer);
                    }
                    else
                    {
                        var node = _localNodes.ElementAt(Random.Value.Next(0,
                            _localNodes.Count));
                        node.Value.Dispatch(itemProducer);
                    }
                }
                else
                {
                    const string message = "There is no node available for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                var availableNodes = _remoteNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        node.Value.Dispatch(itemProducer);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        node.Value.Dispatch(itemProducer);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        node.Value.Dispatch(itemProducer);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                    }
                }
            }
        }

        /// <summary>
        /// Get nodes from the cluster
        /// </summary>
        public IReadOnlyCollection<INode> Nodes
        {
            get
            {
                var localNodes = _localNodes.Select(node => node.Value as INode);
                var remoteNodes = _remoteNodes.Select(node => node.Value as INode);
                return new ReadOnlyCollection<INode>(localNodes.Concat(remoteNodes).ToList());
            }
        }

        /// <summary>
        /// Add a node
        /// </summary>
        /// <param name="host">Specified host if the node is a remote node</param>
        public void AddNode(Host host = null)
        {
            if (ClusterOptions.ExecuteRemotely)
            {
                if (host == null)
                {
                    throw new FluentDispatchException(
                        "Host must be provided.");
                }

                if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                {
                    var remoteNode = (IUnaryDispatcherRemoteNode<TInput>) Activator.CreateInstance(
                        typeof(UnaryParallelDispatcherRemoteNode<TInput>),
                        PersistentCache,
                        Progress,
                        host,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                }
                else if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                {
                    var remoteNode = (IUnaryDispatcherRemoteNode<TInput>) Activator.CreateInstance(
                        typeof(UnarySequentialDispatcherRemoteNode<TInput>),
                        PersistentCache,
                        Progress,
                        host,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                }
                else
                {
                    throw new NotImplementedException(
                        $"{nameof(ClusterProcessingType)} of value {ClusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                }
            }
            else
            {
                if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                {
                    var localNode = (IUnaryDispatcherLocalNode<TInput>) Activator.CreateInstance(
                        typeof(UnaryParallelDispatcherLocalNode<TInput>),
                        PersistentCache,
                        _funcResolver.GetItemFunc(),
                        Progress,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                }
                else if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                {
                    var localNode = (IUnaryDispatcherLocalNode<TInput>) Activator.CreateInstance(
                        typeof(UnarySequentialDispatcherLocalNode<TInput>),
                        PersistentCache,
                        _funcResolver.GetItemFunc(),
                        Progress,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                }
                else
                {
                    throw new NotImplementedException(
                        $"{nameof(ClusterProcessingType)} of value {ClusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                }
            }

            _nodeHealthSubscriptions.Add(_localNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeLocalNodeHealth));
            _nodeHealthSubscriptions.Add(_remoteNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeRemoteNodeHealth));
        }

        /// <summary>
        /// Delete a node
        /// </summary>
        /// <param name="nodeId">Node Id</param>
        public void DeleteNode(Guid nodeId)
        {
            _localNodes.TryRemove(nodeId, out _);
            _remoteNodes.TryRemove(nodeId, out _);
        }

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
                foreach (var node in _localNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var node in _remoteNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var nodeHealthSubscription in _nodeHealthSubscriptions)
                {
                    nodeHealthSubscription?.Dispose();
                }
            }

            Disposed = true;
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// The cluster which is in charge of distributing the load to the configured nodes.
    /// </summary>
    /// <typeparam name="TInput"></typeparam>
    public class DirectCluster<TInput> : ClusterBase, ICluster<TInput>
    {
        /// <summary>
        /// Local nodes of the cluster
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IDirectDispatcherLocalNode<TInput>> _localNodes =
            new ConcurrentDictionary<Guid, IDirectDispatcherLocalNode<TInput>>();

        /// <summary>
        /// Remote nodes of the cluster
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IDirectDispatcherRemoteNode<TInput>> _remoteNodes =
            new ConcurrentDictionary<Guid, IDirectDispatcherRemoteNode<TInput>>();

        /// <summary>
        /// Node health subscriptions
        /// </summary>
        private readonly ConcurrentBag<IDisposable> _nodeHealthSubscriptions = new ConcurrentBag<IDisposable>();

        /// <summary>
        /// <see cref="CircuitBreakerOptions"/>
        /// </summary>
        private readonly CircuitBreakerOptions _circuitBreakerOptions;

        /// <summary>
        /// <see cref="FuncResolver{TInput}"/>
        /// </summary>
        private readonly FuncResolver<TInput> _funcResolver;

        /// <summary>
        /// <see cref="DirectCluster{TInput}"/>
        /// </summary>
        /// <param name="resolver"><see cref="Resolver{TInput}"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="progress"><see cref="Progress{TInput}"/></param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        public DirectCluster(
            FuncResolver<TInput> resolver,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CircuitBreakerOptions> circuitBreakerOptions,
            IProgress<double> progress = null,
            CancellationTokenSource cts = null,
            ILoggerFactory loggerFactory = null) :
            this(resolver, progress, cts, clusterOptions.Value, circuitBreakerOptions.Value,
                loggerFactory)
        {

        }

        /// <summary>
        /// <see cref="DirectCluster{TInput}"/>
        /// </summary>
        /// <param name="funcResolver">Resolve the action to execute asynchronously for each items when they are dequeued.</param>
        /// <param name="progress">Progress of the current bulk</param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        private DirectCluster(
            FuncResolver<TInput> funcResolver,
            IProgress<double> progress,
            CancellationTokenSource cts,
            ClusterOptions clusterOptions,
            CircuitBreakerOptions circuitBreakerOptions,
            ILoggerFactory loggerFactory) : base(progress, cts, clusterOptions,
            loggerFactory == null
                ? NullLogger<Cluster<TInput>>.Instance
                : loggerFactory.CreateLogger<Cluster<TInput>>(), loggerFactory)
        {
            try
            {
                _funcResolver = funcResolver;
                _circuitBreakerOptions = circuitBreakerOptions;
                if (ClusterOptions.ExecuteRemotely)
                {
                    if (!ClusterOptions.Hosts.Any())
                    {
                        throw new FluentDispatchException(
                            "Hosts must be provided.");
                    }

                    foreach (var host in ClusterOptions.Hosts)
                    {
                        if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                        {
                            var remoteNode = (IDirectDispatcherRemoteNode<TInput>) Activator.CreateInstance(
                                typeof(DirectParallelDispatcherRemoteNode<TInput>),
                                PersistentCache,
                                Progress,
                                host,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                        }
                        else if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                        {
                            var remoteNode = (IDirectDispatcherRemoteNode<TInput>) Activator.CreateInstance(
                                typeof(DirectSequentialDispatcherRemoteNode<TInput>),
                                PersistentCache,
                                Progress,
                                host,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                        }
                        else
                        {
                            throw new NotImplementedException(
                                $"{nameof(ClusterProcessingType)} of value {clusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                        }
                    }
                }
                else
                {
                    for (var i = 0; i < clusterOptions.ClusterSize; i++)
                    {
                        if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                        {
                            var localNode = (IDirectDispatcherLocalNode<TInput>) Activator.CreateInstance(
                                typeof(DirectParallelDispatcherLocalNode<TInput>),
                                PersistentCache,
                                funcResolver.GetItemFunc(),
                                Progress,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                        }
                        else if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                        {
                            var localNode = (IDirectDispatcherLocalNode<TInput>) Activator.CreateInstance(
                                typeof(DirectSequentialDispatcherLocalNode<TInput>),
                                PersistentCache,
                                funcResolver.GetItemFunc(),
                                Progress,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                        }
                        else
                        {
                            throw new NotImplementedException(
                                $"{nameof(ClusterProcessingType)} of value {clusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                        }
                    }
                }

                foreach (var node in _localNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeLocalNodeHealth));
                }

                foreach (var node in _remoteNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeRemoteNodeHealth));
                }

                LogClusterOptions(circuitBreakerOptions, _localNodes.Count + _remoteNodes.Count);
                var policyResult = Policy.Handle<Exception>().RetryAsync().ExecuteAndCaptureAsync(async () =>
                {
                    var persistedItems =
                        (await PersistentCache.CacheProvider.RetrieveItemsAsync<TInput>()).ToList();
                    await PersistentCache.CacheProvider.FlushDatabaseAsync();
                    if (persistedItems.Any())
                    {
                        Logger.LogInformation(
                            "Cluster was shutdown while items remained to be processed. Submitting...");
                        foreach (var item in persistedItems)
                        {
                            Dispatch(item);
                        }

                        Logger.LogInformation("Remaining items have been successfully processed.");
                    }
                }).GetAwaiter().GetResult();
                if (policyResult.Outcome == OutcomeType.Failure)
                {
                    Logger.LogError(
                        $"Error while processing previously failed items: {policyResult.FinalException?.Message ?? string.Empty}.");
                }

                Logger.LogInformation("Cluster successfully initialized.");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex.Message);
            }
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeLocalNodeHealth(Guid guid)
        {
            if (_localNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeRemoteNodeHealth(Guid guid)
        {
            if (_remoteNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute cluster health
        /// </summary>
        protected override void ComputeClusterHealth()
        {
            ClusterMetrics.CurrentThroughput = _localNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput) +
                                               _remoteNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput);
            base.ComputeClusterHealth();
        }

        /// <summary>
        /// Dispatch an item to the cluster, to be processed by the configured nodes.
        /// </summary>
        /// <remarks>
        /// This won't block the calling thread and this won't never throw any exception.
        /// A retry and circuit breaker policies will gracefully handle non successful attempts.
        /// </remarks>
        /// <param name="item">The item to process</param>
        public void Dispatch(TInput item)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        node.Value.Dispatch(item);
                    }
                    else
                    {
                        var node = _localNodes.ElementAt(Random.Value.Next(0,
                            _localNodes.Count));
                        node.Value.Dispatch(item);
                    }
                }
                else
                {
                    const string message = "There is no node available for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                var availableNodes = _remoteNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        node.Value.Dispatch(item);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        node.Value.Dispatch(item);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        node.Value.Dispatch(item);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                    }
                }
            }
        }

        /// <summary>
        /// Dispatch an item to the cluster, to be processed by the configured nodes.
        /// </summary>
        /// <remarks>
        /// This won't block the calling thread and this won't never throw any exception.
        /// A retry and circuit breaker policies will gracefully handle non successful attempts.
        /// </remarks>
        /// <param name="itemProducer">The item producer to process</param>
        public void Dispatch(Func<TInput> itemProducer)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        node.Value.Dispatch(itemProducer);
                    }
                    else
                    {
                        var node = _localNodes.ElementAt(Random.Value.Next(0,
                            _localNodes.Count));
                        node.Value.Dispatch(itemProducer);
                    }
                }
                else
                {
                    const string message = "There is no node available for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                var availableNodes = _remoteNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        node.Value.Dispatch(itemProducer);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        node.Value.Dispatch(itemProducer);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        node.Value.Dispatch(itemProducer);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                    }
                }
            }
        }

        /// <summary>
        /// Get nodes from the cluster
        /// </summary>
        public IReadOnlyCollection<INode> Nodes
        {
            get
            {
                var localNodes = _localNodes.Select(node => node.Value as INode);
                var remoteNodes = _remoteNodes.Select(node => node.Value as INode);
                return new ReadOnlyCollection<INode>(localNodes.Concat(remoteNodes).ToList());
            }
        }

        /// <summary>
        /// Add a node
        /// </summary>
        /// <param name="host">Specified host if the node is a remote node</param>
        public void AddNode(Host host = null)
        {
            if (ClusterOptions.ExecuteRemotely)
            {
                if (host == null)
                {
                    throw new FluentDispatchException(
                        "Host must be provided.");
                }

                if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                {
                    var remoteNode = (IDirectDispatcherRemoteNode<TInput>) Activator.CreateInstance(
                        typeof(DirectParallelDispatcherRemoteNode<TInput>),
                        PersistentCache,
                        Progress,
                        host,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                }
                else if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                {
                    var remoteNode = (IDirectDispatcherRemoteNode<TInput>) Activator.CreateInstance(
                        typeof(DirectSequentialDispatcherRemoteNode<TInput>),
                        PersistentCache,
                        Progress,
                        host,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                }
                else
                {
                    throw new NotImplementedException(
                        $"{nameof(ClusterProcessingType)} of value {ClusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                }
            }
            else
            {
                if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                {
                    var localNode = (IDirectDispatcherLocalNode<TInput>) Activator.CreateInstance(
                        typeof(DirectParallelDispatcherLocalNode<TInput>),
                        PersistentCache,
                        _funcResolver.GetItemFunc(),
                        Progress,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                }
                else if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                {
                    var localNode = (IDirectDispatcherLocalNode<TInput>) Activator.CreateInstance(
                        typeof(DirectSequentialDispatcherLocalNode<TInput>),
                        PersistentCache,
                        _funcResolver.GetItemFunc(),
                        Progress,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                }
                else
                {
                    throw new NotImplementedException(
                        $"{nameof(ClusterProcessingType)} of value {ClusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                }
            }

            _nodeHealthSubscriptions.Add(_localNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeLocalNodeHealth));
            _nodeHealthSubscriptions.Add(_remoteNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeRemoteNodeHealth));
        }

        /// <summary>
        /// Delete a node
        /// </summary>
        /// <param name="nodeId">Node Id</param>
        public void DeleteNode(Guid nodeId)
        {
            _localNodes.TryRemove(nodeId, out _);
            _remoteNodes.TryRemove(nodeId, out _);
        }

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
                foreach (var node in _localNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var node in _remoteNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var nodeHealthSubscription in _nodeHealthSubscriptions)
                {
                    nodeHealthSubscription?.Dispose();
                }
            }

            Disposed = true;
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// The cluster which is in charge of distributing the load to the configured nodes.
    /// </summary>
    /// <typeparam name="TInput1"></typeparam>
    /// <typeparam name="TInput2"></typeparam>
    /// <typeparam name="TOutput1"></typeparam>
    /// <typeparam name="TOutput2"></typeparam>
    public class Cluster<TInput1, TInput2, TOutput1, TOutput2> : ClusterBase, ICluster<TInput1, TInput2>
    {
        /// <summary>
        /// Local nodes of the cluster
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IDualDispatcherLocalNode<TInput1, TInput2>> _localNodes =
            new ConcurrentDictionary<Guid, IDualDispatcherLocalNode<TInput1, TInput2>>();

        /// <summary>
        /// Remote nodes of the cluster
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IDualDispatcherRemoteNode<TInput1, TInput2>> _remoteNodes =
            new ConcurrentDictionary<Guid, IDualDispatcherRemoteNode<TInput1, TInput2>>();

        /// <summary>
        /// Node health subscriptions
        /// </summary>
        private readonly ConcurrentBag<IDisposable> _nodeHealthSubscriptions = new ConcurrentBag<IDisposable>();

        /// <summary>
        /// Store the items keys in order to join them on their attributed node
        /// </summary>
        private readonly IMemoryCache _resolverCache;

        /// <summary>
        /// <see cref="MemoryCacheEntryOptions"/>
        /// </summary>
        private readonly MemoryCacheEntryOptions _cacheEntryOptions;

        /// <summary>
        /// <see cref="CircuitBreakerOptions"/>
        /// </summary>
        private readonly CircuitBreakerOptions _circuitBreakerOptions;

        /// <summary>
        /// <see cref="FuncPartialResolver{TInput1,TOutput1}"/>
        /// </summary>
        private readonly FuncPartialResolver<TInput1, TOutput1> _item1PartialResolver;

        /// <summary>
        /// <see cref="FuncPartialResolver{TInput2,TOutput2}"/>
        /// </summary>
        private readonly FuncPartialResolver<TInput2, TOutput2> _item2PartialResolver;

        /// <summary>
        /// <see cref="FuncResolver{TOutput1,TOutput2}"/>
        /// </summary>
        private readonly FuncResolver<TOutput1, TOutput2> _dualResolver;

        /// <summary>
        /// <see cref="Cluster{TInput1, TInput2, TOutput2, TOutput2}"/>
        /// </summary>
        /// <param name="resolverCache"><see cref="IMemoryCache"/></param>
        /// <param name="item1PartialResolver"><see cref="PartialResolver{TInput1,TOutput1}"/></param>
        /// <param name="item2PartialResolver"><see cref="PartialResolver{TInput2,TOutput2}"/></param>
        /// <param name="dualResolver"><see cref="DualResolver{TOutput1,TOutput2}"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="progress"><see cref="Progress{TInput1}"/></param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        public Cluster(
            IMemoryCache resolverCache,
            PartialResolver<TInput1, TOutput1> item1PartialResolver,
            PartialResolver<TInput2, TOutput2> item2PartialResolver,
            DualResolver<TOutput1, TOutput2> dualResolver,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CircuitBreakerOptions> circuitBreakerOptions,
            IProgress<double> progress = null,
            CancellationTokenSource cts = null,
            ILoggerFactory loggerFactory = null) :
            this(resolverCache,
                progress, cts,
                clusterOptions.Value,
                circuitBreakerOptions.Value,
                loggerFactory,
                item1PartialResolver,
                item2PartialResolver,
                dualResolver)
        {

        }

        /// <summary>
        /// <see cref="Cluster{TInput1, TInput2, TOutput2, TOutput2}"/>
        /// </summary>
        /// <param name="resolverCache"><see cref="IMemoryCache"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="progress"><see cref="Progress{TInput1}"/></param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        public Cluster(
            IMemoryCache resolverCache,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CircuitBreakerOptions> circuitBreakerOptions,
            IProgress<double> progress = null,
            CancellationTokenSource cts = null,
            ILoggerFactory loggerFactory = null) :
            this(resolverCache, progress, cts,
                clusterOptions.Value,
                circuitBreakerOptions.Value,
                loggerFactory)
        {

        }

        /// <summary>
        /// <see cref="Cluster{TInput1, TInput2, TOutput2, TOutput2}"/>
        /// </summary>
        /// <param name="resolverCache"><see cref="IMemoryCache"/></param>
        /// <param name="progress">Progress of the current bulk</param>
        /// <param name="cts"><see cref="CancellationTokenSource"/></param>
        /// <param name="clusterOptions"><see cref="ClusterOptions"/></param>
        /// <param name="circuitBreakerOptions"><see cref="CircuitBreakerOptions"/></param>
        /// <param name="loggerFactory"><see cref="ILoggerFactory"/></param>
        /// <param name="item1PartialResolver">Resolve the action to execute asynchronously for each items when they are dequeued.</param>
        /// <param name="item2PartialResolver">Resolve the action to execute asynchronously for each items when they are dequeued.</param>
        /// <param name="dualResolver">Resolve the action to execute asynchronously for each items when they are dequeued.</param>
        private Cluster(
            IMemoryCache resolverCache,
            IProgress<double> progress,
            CancellationTokenSource cts,
            ClusterOptions clusterOptions,
            CircuitBreakerOptions circuitBreakerOptions,
            ILoggerFactory loggerFactory,
            FuncPartialResolver<TInput1, TOutput1> item1PartialResolver = null,
            FuncPartialResolver<TInput2, TOutput2> item2PartialResolver = null,
            FuncResolver<TOutput1, TOutput2> dualResolver = null) : base(progress, cts, clusterOptions,
            loggerFactory == null
                ? NullLogger<Cluster<TInput1, TInput2, TOutput1, TOutput2>>.Instance
                : loggerFactory.CreateLogger<Cluster<TInput1, TInput2, TOutput1, TOutput2>>(), loggerFactory)
        {
            _resolverCache = resolverCache;
            _cacheEntryOptions = new MemoryCacheEntryOptions();
            _cacheEntryOptions
                .SetPriority(CacheItemPriority.Normal)
                .SetSlidingExpiration(TimeSpan.FromMinutes(10))
                .SetSize(1);
            try
            {
                _circuitBreakerOptions = circuitBreakerOptions;
                _item1PartialResolver = item1PartialResolver;
                _item2PartialResolver = item2PartialResolver;
                _dualResolver = dualResolver;
                if (ClusterOptions.ExecuteRemotely)
                {
                    if (!ClusterOptions.Hosts.Any())
                    {
                        throw new FluentDispatchException(
                            "Hosts must be provided.");
                    }

                    foreach (var host in ClusterOptions.Hosts)
                    {
                        if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                        {
                            var remoteNode = (IDualDispatcherRemoteNode<TInput1, TInput2>) Activator.CreateInstance(
                                typeof(DualParallelDispatcherRemoteNode<TInput1, TInput2, TOutput1, TOutput2>),
                                PersistentCache,
                                Progress,
                                host,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                        }
                        else if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                        {
                            var remoteNode = (IDualDispatcherRemoteNode<TInput1, TInput2>) Activator.CreateInstance(
                                typeof(DualSequentialDispatcherRemoteNode<TInput1, TInput2, TOutput1, TOutput2>),
                                PersistentCache,
                                Progress,
                                host,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                        }
                        else
                        {
                            throw new NotImplementedException(
                                $"{nameof(ClusterProcessingType)} of value {clusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                        }
                    }
                }
                else
                {
                    for (var i = 0; i < clusterOptions.ClusterSize; i++)
                    {
                        if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                        {
                            var localNode = (IDualDispatcherLocalNode<TInput1, TInput2>) Activator.CreateInstance(
                                typeof(DualParallelDispatcherLocalNode<TInput1, TInput2, TOutput1, TOutput2>),
                                PersistentCache,
                                item1PartialResolver?.GetItemFunc(),
                                item2PartialResolver?.GetItemFunc(),
                                dualResolver?.GetItemFunc(),
                                Progress,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                        }
                        else if (clusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                        {
                            var localNode = (IDualDispatcherLocalNode<TInput1, TInput2>) Activator.CreateInstance(
                                typeof(DualSequentialDispatcherLocalNode<TInput1, TInput2, TOutput1, TOutput2>),
                                PersistentCache,
                                item1PartialResolver?.GetItemFunc(),
                                item2PartialResolver?.GetItemFunc(),
                                dualResolver?.GetItemFunc(),
                                Progress,
                                CancellationTokenSource,
                                circuitBreakerOptions,
                                clusterOptions,
                                Logger);
                            _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                        }
                        else
                        {
                            throw new NotImplementedException(
                                $"{nameof(ClusterProcessingType)} of value {clusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                        }
                    }
                }

                foreach (var node in _localNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeLocalNodeHealth));
                }

                foreach (var node in _remoteNodes)
                {
                    _nodeHealthSubscriptions.Add(
                        node.Value.NodeMetrics.RefreshSubject.Subscribe(ComputeRemoteNodeHealth));
                }

                LogClusterOptions(circuitBreakerOptions, _localNodes.Count + _remoteNodes.Count);
                var policyResult = Policy.Handle<Exception>().RetryAsync().ExecuteAndCaptureAsync(async () =>
                {
                    var persistedItems1 = (await PersistentCache.CacheProvider.RetrieveItems1Async<TInput1>()).ToList();
                    var persistedItems2 = (await PersistentCache.CacheProvider.RetrieveItems2Async<TInput2>()).ToList();
                    await PersistentCache.CacheProvider.FlushDatabaseAsync();
                    if (persistedItems1.Any() || persistedItems2.Any())
                    {
                        Logger.LogInformation(
                            "Cluster was shutdown while items remained to be processed. Submitting...");
                        foreach (var (key, entity) in persistedItems1)
                        {
                            Dispatch(Guid.Parse(key), entity);
                        }

                        foreach (var (key, entity) in persistedItems2)
                        {
                            Dispatch(Guid.Parse(key), entity);
                        }

                        Logger.LogInformation("Remaining items have been successfully processed.");
                    }
                }).GetAwaiter().GetResult();
                if (policyResult.Outcome == OutcomeType.Failure)
                {
                    Logger.LogError(
                        $"Error while processing previously failed items: {policyResult.FinalException?.Message ?? string.Empty}.");
                }

                Logger.LogInformation("Cluster successfully initialized.");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex.Message);
            }
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeLocalNodeHealth(Guid guid)
        {
            if (_localNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute node health
        /// </summary>
        /// <param name="guid">Node identifier</param>
        private void ComputeRemoteNodeHealth(Guid guid)
        {
            if (_remoteNodes.TryGetValue(guid, out var node))
                ComputeNodeHealth(node.NodeMetrics);
        }

        /// <summary>
        /// Compute cluster health
        /// </summary>
        protected override void ComputeClusterHealth()
        {
            ClusterMetrics.CurrentThroughput = _localNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput) +
                                               _remoteNodes.Sum(node => node.Value.NodeMetrics.CurrentThroughput);
            base.ComputeClusterHealth();
        }

        /// <summary>
        /// Dispatch an item to the cluster, to be processed by the configured nodes.
        /// </summary>
        /// <remarks>
        /// This won't block the calling thread and this won't never throw any exception.
        /// A retry and circuit breaker policies will gracefully handle non successful attempts.
        /// </remarks>
        /// <param name="key">The item identifier</param>
        /// <param name="item">The item to process</param>
        public void Dispatch(Guid key, TInput1 item)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localNodes.Any())
                {
                    if (_resolverCache.TryGetValue(key, out var value) && value is Guid affinityNodeGuid)
                    {
                        var node = _localNodes.FirstOrDefault(n => n.Value.NodeMetrics.Id == affinityNodeGuid);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput1>(key, item, persistentCacheToken);
                        node.Value?.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput1>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else
                    {
                        var node = _localNodes.ElementAt(Random.Value.Next(0,
                            _localNodes.Count));
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput1>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                }
                else
                {
                    const string message = "There is no node available for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                var availableNodes = _remoteNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (_resolverCache.TryGetValue(key, out var value) && value is Guid affinityNodeGuid)
                    {
                        var node = availableNodes.FirstOrDefault(n => n.Value.NodeMetrics.Id == affinityNodeGuid);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput1>(key, item, persistentCacheToken);
                        node.Value?.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput1>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput1>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput1>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                    }
                }
            }
        }

        /// <summary>
        /// Dispatch an item to the cluster, to be processed by the configured nodes.
        /// </summary>
        /// <remarks>
        /// This won't block the calling thread and this won't never throw any exception.
        /// A retry and circuit breaker policies will gracefully handle non successful attempts.
        /// </remarks>
        /// <param name="key">The item identifier</param>
        /// <param name="item">The item to process</param>
        public void Dispatch(Guid key, TInput2 item)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localNodes.Any())
                {
                    if (_resolverCache.TryGetValue(key, out var value) && value is Guid affinityNodeGuid)
                    {
                        var node = _localNodes.FirstOrDefault(n => n.Value.NodeMetrics.Id == affinityNodeGuid);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput2>(key, item, persistentCacheToken);
                        node.Value?.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput2>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else
                    {
                        var node = _localNodes.ElementAt(Random.Value.Next(0,
                            _localNodes.Count));
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput2>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                }
                else
                {
                    const string message = "There is no node available for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                var availableNodes = _remoteNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (_resolverCache.TryGetValue(key, out var value) && value is Guid affinityNodeGuid)
                    {
                        var node = availableNodes.FirstOrDefault(n => n.Value.NodeMetrics.Id == affinityNodeGuid);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput2>(key, item, persistentCacheToken);
                        node.Value?.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput2>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput2>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedItem<TInput2>(key, item, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                    }
                }
            }
        }

        /// <summary>
        /// Dispatch an item to the cluster, to be processed by the configured nodes.
        /// </summary>
        /// <remarks>
        /// This won't block the calling thread and this won't never throw any exception.
        /// A retry and circuit breaker policies will gracefully handle non successful attempts.
        /// </remarks>
        /// <param name="key">The item identifier</param>
        /// <param name="itemProducer">The item to process</param>
        public void Dispatch(Guid key, Func<TInput1> itemProducer)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localNodes.Any())
                {
                    if (_resolverCache.TryGetValue(key, out var value) && value is Guid affinityNodeGuid)
                    {
                        var node = _localNodes.FirstOrDefault(n => n.Value.NodeMetrics.Id == affinityNodeGuid);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput1>(key, itemProducer, persistentCacheToken);
                        node.Value?.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput1>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else
                    {
                        var node = _localNodes.ElementAt(Random.Value.Next(0,
                            _localNodes.Count));
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput1>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                }
                else
                {
                    const string message = "There is no node available for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                var availableNodes = _remoteNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (_resolverCache.TryGetValue(key, out var value) && value is Guid affinityNodeGuid)
                    {
                        var node = availableNodes.FirstOrDefault(n => n.Value.NodeMetrics.Id == affinityNodeGuid);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput1>(key, itemProducer, persistentCacheToken);
                        node.Value?.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput1>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput1>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput1>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                    }
                }
            }
        }

        /// <summary>
        /// Dispatch an item to the cluster, to be processed by the configured nodes.
        /// </summary>
        /// <remarks>
        /// This won't block the calling thread and this won't never throw any exception.
        /// A retry and circuit breaker policies will gracefully handle non successful attempts.
        /// </remarks>
        /// <param name="key">The item identifier</param>
        /// <param name="itemProducer">The item to process</param>
        public void Dispatch(Guid key, Func<TInput2> itemProducer)
        {
            if (!ClusterOptions.ExecuteRemotely)
            {
                if (_localNodes.Any())
                {
                    if (_resolverCache.TryGetValue(key, out var value) && value is Guid affinityNodeGuid)
                    {
                        var node = _localNodes.FirstOrDefault(n => n.Value.NodeMetrics.Id == affinityNodeGuid);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput2>(key, itemProducer, persistentCacheToken);
                        node.Value?.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = _localNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput2>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else
                    {
                        var node = _localNodes.ElementAt(Random.Value.Next(0,
                            _localNodes.Count));
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput2>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                }
                else
                {
                    const string message = "There is no node available for the item to be processed.";
                    Logger.LogError(message);
                    throw new FluentDispatchException(message);
                }
            }
            else
            {
                var availableNodes = _remoteNodes
                    .Where(node =>
                        node.Value.NodeMetrics.Alive &&
                        (!ClusterOptions.EvictItemsWhenNodesAreFull || !node.Value.NodeMetrics.Full))
                    .ToList();
                if (availableNodes.Any())
                {
                    if (_resolverCache.TryGetValue(key, out var value) && value is Guid affinityNodeGuid)
                    {
                        var node = availableNodes.FirstOrDefault(n => n.Value.NodeMetrics.Id == affinityNodeGuid);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput2>(key, itemProducer, persistentCacheToken);
                        node.Value?.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.BestEffort)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.TotalItemsProcessed <= node2.Value.NodeMetrics.TotalItemsProcessed
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput2>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Randomized)
                    {
                        var node = availableNodes.ElementAt(Random.Value.Next(0,
                            availableNodes.Count));
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput2>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else if (ClusterOptions.NodeQueuingStrategy == NodeQueuingStrategy.Healthiest)
                    {
                        var node = availableNodes.Aggregate((node1, node2) =>
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Any(counter => counter.Key == "CPU Usage") &&
                            node1.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value <=
                            node2.Value.NodeMetrics.RemoteNodeHealth.PerformanceCounters
                                .Single(counter => counter.Key == "CPU Usage").Value
                                ? node1
                                : node2);
                        _resolverCache.Set(key, node.Value.NodeMetrics.Id, _cacheEntryOptions);
                        var persistentCacheToken = new CancellationTokenSource();
                        var persistentItem = new LinkedFuncItem<TInput2>(key, itemProducer, persistentCacheToken);
                        node.Value.Dispatch(persistentItem);
                    }
                    else
                    {
                        throw new NotImplementedException(
                            $"{nameof(NodeQueuingStrategy)} of value {ClusterOptions.NodeQueuingStrategy.ToString()} is not implemented.");
                    }
                }
                else
                {
                    if (_remoteNodes.All(node => !node.Value.NodeMetrics.Alive))
                    {
                        const string message = "Could not dispatch item, nodes are offline.";
                        Logger.LogError(message);
                        throw new FluentDispatchException(message);
                    }
                    else
                    {
                        const string message = "Could not dispatch item, nodes are full.";
                        Logger.LogWarning(message);
                    }
                }
            }
        }

        /// <summary>
        /// Get nodes from the cluster
        /// </summary>
        public IReadOnlyCollection<INode> Nodes
        {
            get
            {
                var localNodes = _localNodes.Select(node => node.Value as INode);
                var remoteNodes = _remoteNodes.Select(node => node.Value as INode);
                return new ReadOnlyCollection<INode>(localNodes.Concat(remoteNodes).ToList());
            }
        }

        /// <summary>
        /// Add a node
        /// </summary>
        /// <param name="host">Specified host if the node is a remote node</param>
        public void AddNode(Host host = null)
        {
            if (ClusterOptions.ExecuteRemotely)
            {
                if (host == null)
                {
                    throw new FluentDispatchException(
                        "Host must be provided.");
                }

                if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                {
                    var remoteNode = (IDualDispatcherRemoteNode<TInput1, TInput2>) Activator.CreateInstance(
                        typeof(DualParallelDispatcherRemoteNode<TInput1, TInput2, TOutput1, TOutput2>),
                        PersistentCache,
                        Progress,
                        host,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                }
                else if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                {
                    var remoteNode = (IDualDispatcherRemoteNode<TInput1, TInput2>) Activator.CreateInstance(
                        typeof(DualSequentialDispatcherRemoteNode<TInput1, TInput2, TOutput1, TOutput2>),
                        PersistentCache,
                        Progress,
                        host,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _remoteNodes.TryAdd(remoteNode.NodeMetrics.Id, remoteNode);
                }
                else
                {
                    throw new NotImplementedException(
                        $"{nameof(ClusterProcessingType)} of value {ClusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                }
            }
            else
            {
                if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Parallel)
                {
                    var localNode = (IDualDispatcherLocalNode<TInput1, TInput2>) Activator.CreateInstance(
                        typeof(DualParallelDispatcherLocalNode<TInput1, TInput2, TOutput1, TOutput2>),
                        PersistentCache,
                        _item1PartialResolver?.GetItemFunc(),
                        _item2PartialResolver?.GetItemFunc(),
                        _dualResolver?.GetItemFunc(),
                        Progress,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                }
                else if (ClusterOptions.ClusterProcessingType == ClusterProcessingType.Sequential)
                {
                    var localNode = (IDualDispatcherLocalNode<TInput1, TInput2>) Activator.CreateInstance(
                        typeof(DualSequentialDispatcherLocalNode<TInput1, TInput2, TOutput1, TOutput2>),
                        PersistentCache,
                        _item1PartialResolver?.GetItemFunc(),
                        _item2PartialResolver?.GetItemFunc(),
                        _dualResolver?.GetItemFunc(),
                        Progress,
                        CancellationTokenSource,
                        _circuitBreakerOptions,
                        ClusterOptions,
                        Logger);
                    _localNodes.TryAdd(localNode.NodeMetrics.Id, localNode);
                }
                else
                {
                    throw new NotImplementedException(
                        $"{nameof(ClusterProcessingType)} of value {ClusterOptions.ClusterProcessingType.ToString()} is not implemented.");
                }
            }

            _nodeHealthSubscriptions.Add(_localNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeLocalNodeHealth));
            _nodeHealthSubscriptions.Add(_remoteNodes.Last().Value.NodeMetrics.RefreshSubject
                .Subscribe(ComputeRemoteNodeHealth));
        }

        /// <summary>
        /// Delete a node
        /// </summary>
        /// <param name="nodeId">Node Id</param>
        public void DeleteNode(Guid nodeId)
        {
            _localNodes.TryRemove(nodeId, out _);
            _remoteNodes.TryRemove(nodeId, out _);
        }

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
                foreach (var node in _localNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var node in _remoteNodes)
                {
                    node.Value?.Dispose();
                }

                foreach (var nodeHealthSubscription in _nodeHealthSubscriptions)
                {
                    nodeHealthSubscription?.Dispose();
                }
            }

            Disposed = true;
            base.Dispose(disposing);
        }
    }
}