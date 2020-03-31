using System;
using System.Threading;
using System.Threading.Tasks;

namespace FluentDispatch.Extensions
{
    internal static class TaskExtensions
    {
        public static Task WrapTaskForCancellation(this Task task, CancellationToken ct)
        {
            if (ct.CanBeCanceled && !task.IsCompleted)
            {
                var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                if (ct.IsCancellationRequested)
                {
                    tcs.TrySetCanceled();
                }
                else
                {
                    ct.Register(() => { tcs.TrySetCanceled(); });
                    task.ContinueWith(antecedent =>
                    {
                        if (antecedent.IsFaulted)
                        {
                            var exception = antecedent.Exception?.GetBaseException();
                            tcs.TrySetException(exception ??
                                                new Exception("Task is faulted but no exception has been retrieved."));
                        }
                        else if (antecedent.IsCanceled)
                        {
                            tcs.TrySetCanceled();
                        }
                        else
                        {
                            tcs.TrySetResult(true);
                        }
                    }, TaskContinuationOptions.ExecuteSynchronously);
                }

                return tcs.Task;
            }

            return task;
        }

        public static Task<T> WrapTaskForCancellation<T>(this Task<T> task, CancellationToken ct)
        {
            if (ct.CanBeCanceled && !task.IsCompleted)
            {
                var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
                if (ct.IsCancellationRequested)
                {
                    tcs.TrySetCanceled();
                }
                else
                {
                    ct.Register(() => { tcs.TrySetCanceled(); });
                    task.ContinueWith(antecedent =>
                    {
                        if (antecedent.IsFaulted)
                        {
                            var exception = antecedent.Exception?.GetBaseException();
                            tcs.TrySetException(exception ??
                                                new Exception("Task is faulted but no exception has been retrieved."));
                        }
                        else if (antecedent.IsCanceled)
                        {
                            tcs.TrySetCanceled();
                        }
                        else
                        {
                            tcs.TrySetResult(antecedent.Result);
                        }
                    }, TaskContinuationOptions.ExecuteSynchronously);
                }

                return tcs.Task;
            }

            return task;
        }
    }
}