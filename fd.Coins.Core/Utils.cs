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

        public static double ToSignificantFigures(this double i)
        {
            var f = Math.Floor(Math.Log10(Math.Abs(i)) + 1);
            while (i >= 100)
            {
                i /= 10;
            }
            return (i * Math.Pow(10, f - 2)) < 1 ? 0 : (i * Math.Pow(10, f - 2));
        }

        public static double RoundToSignificant(this long i)
        {
            var f = Math.Floor(Math.Log10(Math.Abs(i)) + 1);
            while (i >= 100)
            {
                i /= 10;
            }
            return (i * Math.Pow(10, f - 2)) < 1 ? 0 : (i * Math.Pow(10, f - 2));
        }

        public static double ToSignificantFigures(this int i)
        {
            var f = Math.Floor(Math.Log10(Math.Abs(i)) + 1);
            while (i >= 100)
            {
                i /= 10;
            }
            return (i * Math.Pow(10, f - 2)) < 1 ? 0 : (i * Math.Pow(10, f - 2));
        }
    }
}
