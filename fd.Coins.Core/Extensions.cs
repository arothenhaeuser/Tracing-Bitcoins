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
            var ret = self.FirstOrDefault();
            if(ret == null)
            {
                ret = db.Create.Vertex<OVertex>().Set("hash", "coinbase").Run();
            }
            return ret;
        }
    }
}
