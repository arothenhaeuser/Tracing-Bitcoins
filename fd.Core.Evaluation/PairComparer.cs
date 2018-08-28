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
            return GetHashCode(x) == GetHashCode(y); ;
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
