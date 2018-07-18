using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                }
                catch (Exception e)
                {
                    if(e.Message.Contains("ConcurrentModificationException"))
                    count--;
                }
            }
        }
    }
}
