using System;

namespace fd.Coins.Core
{
    public static class Utils
    {
        public static void RetryOnConcurrentFail(int attempts, Func<bool> p)
        {
            var count = attempts;
            while (count > 0)
            {
                try
                {
                    if (p.Invoke())
                        return;
                    else
                        count--;
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("ConcurrentModificationException"))
                        count--;
                    else
                        throw;
                }
            }
            throw new InvalidOperationException($"Operation failed after {attempts} attempts.");
        }
    }
}
