using Orient.Client;
using System;

namespace fd.Coins.Clustering
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
                }
                catch (Exception e)
                {
                    count--;
                }
            }
        }
    }
}
