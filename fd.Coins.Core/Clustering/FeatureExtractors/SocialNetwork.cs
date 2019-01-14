using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;

namespace fd.Coins.Core.Clustering.FeatureExtractors
{
    /// <summary>
    /// Feature Extractor: Extracts the social network from transactions.
    /// Outputs two lists, one for payers, one for payees.
    /// Example:
    /// TX:     (A) ->  XXXX   ->  C
    ///         B   ->  XXXX   ->  D
    /// feat:   [B][C,D]
    /// </summary>
    public class SocialNetwork : Extractor
    {
        private Dictionary<string, List<string>> _result;

        public SocialNetwork()
        {
            _result = new Dictionary<string, List<string>>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result[addr1];
            var v2 = _result[addr2];
            var numerator = (double)v1.Intersect(v2).Count();
            var denominator = v1.Union(v2).Count();
            var res = 1 - (numerator / denominator);
            return Double.IsNaN(res) ? 0 : res;
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                foreach (var address in addresses)
                {
                    var kvp = new KeyValuePair<string, List<string>>(address, mainDB.Query($"SELECT set(network) as network FROM (SELECT unionAll($a, $b, $c) as network FROM Link LET $a = inV().inE().tAddr, $b = inV().outE().tAddr, $c = outV().inE().tAddr WHERE tAddr = '{address}' LIMIT 10)").First().GetField<List<string>>("network"));
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
