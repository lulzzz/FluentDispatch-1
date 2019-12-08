using System;

namespace FluentDispatch.Processors.Direct
{
    internal interface IDirectProcessor<in TInput> : IDisposable
    {
        void Add(TInput item);

        void Add(Func<TInput> item);
    }
}
