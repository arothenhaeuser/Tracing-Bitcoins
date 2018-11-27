using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    public class Amounts : Clustering
    {
        private Dictionary<string, double> _result;

        public Amounts()
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
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            using (var mainDB = new ODatabase(mainOptions))
            {
                foreach (var address in addresses)
                {
                    var kvp = mainDB.Query($"SELECT avg(amount).asLong() as total FROM Link WHERE tAddr = '{address}'").Select(x => new KeyValuePair<string, double>(address, (double)x.GetField<long>("total"))).First();
                    try
                    {
                        _result.Add(kvp.Key, kvp.Value);
                    }
                    catch
                    {
                        Console.WriteLine(address + " not in DB?");
                    }
                    Console.WriteLine(address + " done.");
                }
            }
            sw.Stop();
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
        }

        private void AddToResult(Dictionary<long, List<string>> query)
        {
            foreach (var kvp in query)
            {
                foreach (var address in kvp.Value)
                {
                    try
                    {
                        _result.Add(address, kvp.Key);
                    }
                    catch (ArgumentException)
                    {
                        _result[address] = (_result[address] + kvp.Key) / 2;
                    }
                }
            }
        }
    }
}
