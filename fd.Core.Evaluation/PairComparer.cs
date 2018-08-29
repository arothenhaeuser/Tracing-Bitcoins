using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Evaluation
{
    public class PairComparer<T> : IEqualityComparer<T[]>
    {
        // pairs need to be sorted
        public bool Equals(T[] x, T[] y)
        {
            var r1 = Comparer<T>.Default.Compare(x[0], y[0]) == 0;
            var r2 = Comparer<T>.Default.Compare(x[1], y[1]) == 0;
            return r1 && r2;
        }

        public int GetHashCode(T[] x)
        {
            var h = 0;
            foreach (var elem in x)
            {
                h = h ^ elem.GetHashCode();
            }
            return h;
        }
    }
}
