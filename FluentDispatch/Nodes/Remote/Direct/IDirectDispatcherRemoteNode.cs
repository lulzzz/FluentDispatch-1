using System;

namespace FluentDispatch.Nodes.Remote.Direct
{
    internal interface IDirectDispatcherRemoteNode<in TInput> : INode, IDisposable
    {
        /// <summary>
        /// Dispatch a <see cref="TInput"/> to the node.
        /// </summary>
        /// <param name="item"><see cref="TInput"/> to broadcast</param>
        void Dispatch(TInput item);

        /// <summary>
        /// Dispatch a <see cref="TInput"/> to the node.
        /// </summary>
        /// <param name="item"><see cref="Func{TResult}"/> to broadcast</param>
        void Dispatch(Func<TInput> item);
    }
}