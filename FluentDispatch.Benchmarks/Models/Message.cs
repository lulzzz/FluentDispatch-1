using System.Collections.Concurrent;
using System.Threading;

namespace FluentDispatch.Benchmarks.Models
{
    public class Message
    {
        public long Target { get; }
        public ConcurrentBag<long> Body { get; }
        public SemaphoreSlim SemaphoreSlim { get; }

        public Message(long target, ConcurrentBag<long> body, SemaphoreSlim semaphoreSlim)
        {
            Target = target;
            Body = body;
            SemaphoreSlim = semaphoreSlim;
        }
    }
}