using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Evaluation
{
    public class ArrayComparer<T> : IEqualityComparer<T[]>
    {
        public bool Equals(T[] x, T[] y)
        {
            if (x.SequenceEqual(y)) return true;
            Array.Sort(x);
            Array.Sort(y);
            return x.SequenceEqual(y);
        }

        public int GetHashCode(T[] x)
        {
            Array.Sort(x);
            var h = 0;
            foreach (var elem in x)
            {
                h = h ^ x.GetHashCode();
            }
            return h;
        }
    }
}
