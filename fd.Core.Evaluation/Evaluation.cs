using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Evaluation
{
    public static class Evaluation
    {
        public static double RandIndex<T>(IEnumerable<IEnumerable<T>> gold, IEnumerable<IEnumerable<T>> clustering)
        {
            var known = gold.SelectMany(x => x).Intersect(clustering.SelectMany(x => x));
            var rg = gold.Select(x => x.Intersect(known)).Where(x => x.Count() > 1);
            var rc = clustering.Select(x => x.Intersect(known)).Where(x => x.Count() > 1);
            var pairs1 = rg.SelectMany(c => c.SelectMany(x => c, (x, y) => new T[] { x, y })).Where(p => Comparer<T>.Default.Compare(p[0], p[1]) < 0);
            var pairs2 = rc.SelectMany(c => c.SelectMany(x => c, (x, y) => new T[] { x, y })).Where(p => Comparer<T>.Default.Compare(p[0], p[1]) < 0);

            var n = gold.SelectMany(x => x).Union(rc.SelectMany(x => x)).Count();
            double N = n * (n - 1) / 2;
            //together in both
            double a = pairs1.Intersect(pairs2, new PairComparer<T>()).Count();
            //separate in both
            var b = N - (pairs1.Count() + pairs2.Count() - a);

            return (a + b) / N;
        }

        public static double AdjustedRandIndex<T>(IEnumerable<IEnumerable<T>> gold, IEnumerable<IEnumerable<T>> clustering)
        {
            // get list of all elements occurring in both clusterings
            var known = gold.SelectMany(x => x).Intersect(clustering.SelectMany(x => x));
            // remove elements not occurring in other clustering
            var rg = gold.Select(x => x.Intersect(known));
            var rc = clustering.Select(x => x.Intersect(known));
            // get dimensions of contingency table
            var x1 = rg.Count();
            var y1 = rc.Count();
            // get both clusterings as array of enumerables
            var c1 = rg.ToArray();
            var c2 = rc.ToArray();
            // initialize contingency table
            var contingencyTable = new int[x1][];
            for (var i = 0; i < x1; i++)
            {
                contingencyTable[i] = new int[y1];
            }
            double A = 0, B = 0, C = 0, D = 0;
            for (var i = 0; i < x1; i++)
            {
                for (var j = 0; j < y1; j++)
                {
                    contingencyTable[i][j] = c1[i].Intersect(c2[j]).Count();
                    C += NChooseK(contingencyTable[i][j], 2);
                }
                A += NChooseK(contingencyTable[i].Sum(), 2);
            }
            for(var j = 0; j < y1; j++)
            {
                B += NChooseK(contingencyTable.Select(row => row[j]).Sum(), 2);
            }
            var n = contingencyTable.SelectMany(items => items).Sum();
            D = NChooseK(n, 2);
            var index = C;
            var expected = A * B / D;
            var max = (A + B) / 2;
            return (C-(A*B/D)) / (((A+B)/2)-(A * B /D));
        }

        public static double Accuracy<T>(IEnumerable<IEnumerable<T>> clustering, IEnumerable<IEnumerable<T>> gold)
        {
            var x = clustering.Count();
            var y = gold.Count();
            var c1 = clustering.ToArray();
            var c2 = gold.ToArray();
            var contingencyTable = new int[x][];
            for (var i = 0; i < x; i++)
            {
                contingencyTable[i] = new int[y];
            }
            double TP = 0, TN = 0, FP = 0, FN = 0;
            for (var i = 0; i < x; i++)
            {
                for (var j = 0; j < y; j++)
                {
                    contingencyTable[i][j] = c1[i].Intersect(c2[j]).Count();
                    if (i == j)
                    {
                        TP += contingencyTable[i][j];
                        TN += contingencyTable[i][j];
                    }
                    else
                    {
                        FN += contingencyTable[i][j];
                        FP += contingencyTable[i][j];
                    }
                }
            }
            return (TP + TN) / (TP + TN + FP + FN);
        }

        public static double Precision<T>(IEnumerable<IEnumerable<T>> clustering, IEnumerable<IEnumerable<T>> gold)
        {
            var x = clustering.Count();
            var y = gold.Count();
            var c1 = clustering.ToArray();
            var c2 = gold.ToArray();
            var contingencyTable = new int[x][];
            for (var i = 0; i < x; i++)
            {
                contingencyTable[i] = new int[y];
            }
            double TP = 0, FP = 0;
            for (var i = 0; i < x; i++)
            {
                for (var j = 0; j < y; j++)
                {
                    contingencyTable[i][j] = c1[i].Intersect(c2[j]).Count();
                    if (i == j)
                    {
                        TP += contingencyTable[i][j];
                    }
                    else
                    {
                        FP += contingencyTable[i][j];
                    }
                }
            }
            return (TP) / (TP + FP);
        }

        public static double Recall<T>(IEnumerable<IEnumerable<T>> clustering, IEnumerable<IEnumerable<T>> gold)
        {
            var x = clustering.Count();
            var y = gold.Count();
            var c1 = clustering.ToArray();
            var c2 = gold.ToArray();
            var contingencyTable = new int[x][];
            for (var i = 0; i < x; i++)
            {
                contingencyTable[i] = new int[y];
            }
            double TP = 0, FN = 0;
            for (var i = 0; i < x; i++)
            {
                for (var j = 0; j < y; j++)
                {
                    contingencyTable[i][j] = c1[i].Intersect(c2[j]).Count();
                    if (i == j)
                    {
                        TP += contingencyTable[i][j];
                    }
                    else
                    {
                        FN += contingencyTable[i][j];
                    }
                }
            }
            return (TP) / (TP + FN);
        }

        public static double F1<T>(IEnumerable<IEnumerable<T>> clustering, IEnumerable<IEnumerable<T>> gold)
        {
            var p = Precision(clustering, gold);
            var r = Recall(clustering, gold);
            return 2 * p * r / (p + r);
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
