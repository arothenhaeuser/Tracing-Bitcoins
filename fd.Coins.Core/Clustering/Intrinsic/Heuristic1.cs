using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    // heuristic #1: all addresses that are input to the same transaction are considered to belong to the same owner
    public class Heuristic1 : Clustering
    {
        private List<List<string>> _result;
        private ConnectionOptions _options;
        public Heuristic1()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "FistfulH1";
            _options.DatabaseType = ODatabaseType.Graph;
            _options.HostName = "localhost";
            _options.Password = "admin";
            _options.Port = 2424;
            _options.UserName = "admin";

            _result = new List<List<string>>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result.FirstOrDefault(x => x.Contains(addr1));
            var v2 = _result.FirstOrDefault(x => x.Contains(addr2));
            return (v1 != null && v2 != null && v1.SequenceEqual(v2)) ? 0.0 : 1.0;
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            var inGroups = new List<List<string>>();
            using (var mainDB = new ODatabase(mainOptions))
            {
                foreach (var address in addresses)
                {
                    var groups = mainDB.Query($"SELECT inE().tAddr AS address FROM (SELECT expand(inV) FROM (SELECT inV() FROM Link WHERE tAddr = '{address}' LIMIT 10000))").SelectMany(x => x.GetField<List<string>>("address")).Where(y => !string.IsNullOrEmpty(y)).Where(x => x.Count() > 1).Distinct().ToList();
                    try
                    {
                        inGroups.Add(groups);
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(address + " not in DB?");
                    }
                    Console.WriteLine(address + " done.");
                }
            }
            var cc = new ClusteringCollapser();
            cc.Collapse(inGroups);
            _result = cc.Clustering;
            sw.Stop();
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
        }

        private List<string> Sort(List<string> items)
        {
            items.Sort();
            return items;
        }
    }
}
