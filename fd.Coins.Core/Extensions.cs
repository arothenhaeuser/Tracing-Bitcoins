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
        public static OVertex FirstOrCoinbase(this List<OVertex> self, ODatabase db, long amount)
        {
            var ret = self.FirstOrDefault();
            if(ret == null)
            {
                ret = db.Create.Vertex("Transaction").Set("Hash", "coinbase").Set("amount", amount).Run();
            }
            return ret;
        }

        public static bool IsCoinBase(this OVertex self)
        {
            return self.GetField<string>("Hash").Equals("coinbase");
        }
    }
}
