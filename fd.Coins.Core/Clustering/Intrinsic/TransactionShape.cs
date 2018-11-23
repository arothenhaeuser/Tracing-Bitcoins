using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    public class TransactionShape : Clustering
    {
        private Dictionary<string, int[]> _result;

        public TransactionShape()
        {
            _result = new Dictionary<string, int[]>();
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            using (var mainDB = new ODatabase(mainOptions))
            {
                _result = mainDB.Query($"SELECT tAddr AS address, eval('list(inV().inE()).size() / count(*)') AS inC, eval('list(inV().outE()).size() / count(*)') AS outC FROM Link WHERE tAddr IN [{string.Join(", ", addresses.Select(x => "'" + x + "'"))}] GROUP BY tAddr").ToDictionary(x => x.GetField<string>("address"), y => new int[] { (int)y.GetField<long>("inC"), (int)y.GetField<long>("outC") });
            }
            sw.Stop();
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
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
    }
}
