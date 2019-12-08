using System.Runtime.CompilerServices;

namespace FluentDispatch.Benchmarks
{
    internal class Helper
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long FindPrimeNumber(int n)
        {
            var count = 0;
            long a = 2;
            while (count < n)
            {
                long b = 2;
                var prime = 1;
                while (b * b <= a)
                {
                    if (a % b == 0)
                    {
                        prime = 0;
                        break;
                    }

                    b++;
                }

                if (prime > 0)
                {
                    count++;
                }

                a++;
            }

            return (--a);
        }
    }
}