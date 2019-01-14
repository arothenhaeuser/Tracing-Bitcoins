using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Core.Clustering.FeatureExtractors
{
    public class TransactionShape : Extractor
    {
        private Dictionary<string, int[]> _result;

        public TransactionShape()
        {
            _result = new Dictionary<string, int[]>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result[addr1];
            var v2 = _result[addr2];
            var numerator = Math.Sqrt(Math.Pow(v1[0] - v2[0], 2) + Math.Pow(v1[1] - v2[1], 2));
            var denominator = v1.Sum() + v2.Sum();
            var res = numerator / denominator;
            return Double.IsNaN(res) ? 0 : res;
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                foreach (var address in addresses)
                {
                    var kvp = new KeyValuePair<string, int[]>(address, mainDB.Query($"SELECT avg(count), avg(count2) FROM (SELECT count(inE()), count(outE()) FROM (SELECT expand(inV) FROM (SELECT inV() FROM Link WHERE tAddr = '{address}' LIMIT 500)) GROUP BY Hash)").Select(x => new int[] { (int)x.GetField<long>("avg"), (int)x.GetField<long>("avg2") }).First());
                    try
                    {
                        _result.Add(kvp.Key, kvp.Value);
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(address + " not in DB?");
                    }
                    Console.WriteLine(address + " done.");
                }
            }
        }
    }
}
