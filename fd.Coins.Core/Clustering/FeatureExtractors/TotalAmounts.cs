using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Core.Clustering.FeatureExtractors
{
    public class TotalAmounts : Extractor
    {
        private Dictionary<string, double> _result;

        public TotalAmounts()
        {
            _result = new Dictionary<string, double>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result[addr1];
            var v2 = _result[addr2];
            var numerator = Math.Abs(v1 - v2);
            var denominator = (v1 + v2);
            var res = numerator / denominator;
            return double.IsNaN(res) ? 0 : res;
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                foreach(var address in addresses)
                {
                    var kvp = new KeyValuePair<string, double>(address, mainDB.Query($"SELECT avg(inE().amount).asLong() as total FROM (SELECT expand(inV) FROM (SELECT inV() FROM Link WHERE tAddr = '{address}' LIMIT 500))").First().GetField<long>("total"));
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
