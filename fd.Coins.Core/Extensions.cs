using Orient.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fd.Coins.Core
{
    public static class Extensions
    {
        public static ODocument FirstOrCoinbase(this List<OVertex> self, string hash, ODatabase db)
        {
            return self.FirstOrDefault() ?? db.Select().From("E").Where("hash").Equals(hash).ToList<OVertex>().First();
        }
    }
}
