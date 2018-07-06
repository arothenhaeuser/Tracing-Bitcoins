using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Evaluation
{
    public static class Evaluation
    {
        public static double RandIndex<T>(IEnumerable<IEnumerable<T>> clustering1, IEnumerable<IEnumerable<T>> clustering2)
        {
            var pairs1 = clustering1.SelectMany(c => c.SelectMany(x => c, (x, y) => new T[] { x, y })).Where(p => Comparer<T>.Default.Compare(p[0], p[1]) < 0);
            var pairs2 = clustering2.SelectMany(c => c.SelectMany(x => c, (x, y) => new T[] { x, y })).Where(p => Comparer<T>.Default.Compare(p[0], p[1]) < 0);

            var n = clustering1.SelectMany(x => x).Union(clustering2.SelectMany(x => x)).Count();
            double N = n * (n - 1) / 2;

            double a = pairs1.Intersect(pairs2, new ArrayComparer<T>()).Count();
            var b = N - pairs1.Union(pairs2, new ArrayComparer<T>()).Count();

            return (a + b) / N;
        }

        public static double AdjustedRandIndex<T>(IEnumerable<IEnumerable<T>> clustering1, IEnumerable<IEnumerable<T>> clustering2)
        {
            var x = clustering1.Count();
            var y = clustering2.Count();
            var c1 = clustering1.ToArray();
            var c2 = clustering2.ToArray();
            var n = clustering1.SelectMany(item => item).Union(clustering2.SelectMany(item => item)).Count();
            var contingencyTable = new int[x][];
            for (var i = 0; i < x; i++)
            {
                contingencyTable[i] = new int[y];
            }
            double A = 0, B = 0, C = 0, D = 0;
            for (var i = 0; i < x; i++)
            {
                for (var j = 0; j < y; j++)
                {
                    contingencyTable[i][j] = c1[i].Intersect(c2[j]).Count();
                    C += NChooseK(contingencyTable[i][j], 2);
                }
                A += NChooseK(contingencyTable[i].Sum(), 2);
            }
            for(int j = 0; j < y; j++)
            {
                B += NChooseK(contingencyTable.Select(row => row[j]).Sum(), 2);
            }
            D = NChooseK(n, 2);
            return (C-(A*B/D))/(((A+B)/2)-(A * B /D));
        }

        public static double NChooseK(int n, int k)
        {
            if (n == 0) return 0;
            if (n < k) return 0;
            if (n == k) return 1;
            double sum = 0;
            for (long i = 0; i < k; i++)
            {
                sum += Math.Log10(n - i);
                sum -= Math.Log10(i + 1);
            }
            return Math.Pow(10, sum);
        }
    }
}
