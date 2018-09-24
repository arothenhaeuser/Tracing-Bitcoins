using Orient.Client;
using Orient.Client.API;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

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
            return self.GetField<bool>("Coinbase");
        }

        public static Dictionary<string, BitArray> ToSlots(this Dictionary<long, List<string>> timeClusters, int count)
        {
            var test = new Dictionary<string, BitArray>();
            foreach (var timeCluster in timeClusters)
            {
                foreach (var address in timeCluster.Value)
                {
                    var slots = new BitArray(count);
                    slots.Set(checked((int)timeCluster.Key), true);
                    try
                    {
                        test.Add(address, slots);
                    }
                    catch (ArgumentException)
                    {
                        test[address] = test[address].Or(slots);
                    }
                }
            }
            return test;
        }

        public static Dictionary<string, double> ToAverage(this Dictionary<long, List<string>> amountClusters)
        {
            var test = new Dictionary<string, double>();
            foreach (var amountCluster in amountClusters)
            {
                foreach (var address in amountCluster.Value)
                {
                    try
                    {
                        test.Add(address, amountCluster.Key);
                    }
                    catch (ArgumentException)
                    {
                        test[address] = (test[address] + amountCluster.Key) / 2;
                    }
                }
            }
            return test;
        }

        public static void AddEdgeIfNotExists(this OTransaction self, OEdge edge, OVertex from, OVertex to)
        {
            if(!(from.HasField("out_" + edge.OClassName) && to.HasField("in_" + edge.OClassName)))
            {
                self.AddEdge(edge, from, to);
            }
        }
    }
}
