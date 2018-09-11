using fd.Coins.Core.Clustering.Intrinsic;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.Clustering
{
    class Program
    {
        static void Main(string[] args)
        {
            var txgraphOptions = new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" };
            var algoPipe = new List<Core.Clustering.Clustering>();
            algoPipe.Add(new TotalAmounts());
            algoPipe.Add(new TimeSlots());
            algoPipe.Add(new Core.Clustering.Intrinsic.DayOfWeek());
            algoPipe.Add(new Heuristic1());
            algoPipe.Add(new Heuristic2());

            using (var txgraph = new ODatabase(txgraphOptions))
            {
                long skip = 0;
                long limit = 50000;
                long total = txgraph.CountRecords;

                while (skip < 1000000/*total*/)
                {
                    var rids = txgraph.Command($"SELECT @rid FROM Transaction SKIP {skip} LIMIT {limit}").ToList().Select(x => x.GetField<ORID>("rid")).ToList();
                    skip += limit;

                    var tasks = new List<Task>();
                    foreach(var algo in algoPipe)
                    {
                        tasks.Add(Task.Run(() =>
                        {
                            algo.Run(txgraphOptions, rids);
                        }));
                    }
                    Task.WaitAll(tasks.ToArray());
                    Console.WriteLine($"{skip} processed.");
                }

                var result = new Dictionary<string, double>();
                var addresses = GetAddresses();
                foreach(var addr in addresses)
                {
                    var distance = 0.0;
                    foreach (var algo in algoPipe)
                    {
                        distance += algo.Distance(addr, "1dicec9k7KpmQaA8Uc8aCCxfWnwEWzpXE");
                    }
                    distance /= algoPipe.Count;
                    result.Add(addr, distance);
                }
                Console.WriteLine(string.Join("\n", result.OrderBy(x => x.Value).Select(x => string.Join(",", x.Key, x.Value))));
            }

            Console.Read();
        }

        private static List<string> GetAddresses()
        {
            var txgraphOptions = new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" };
            using (var txgraph = new ODatabase(txgraphOptions))
            {
                return txgraph.Command($"SELECT list(inE().tAddr) AS addresses FROM (SELECT * FROM Transaction SKIP 3772550 LIMIT 500)").ToList().FirstOrDefault().GetField<List<string>>("addresses").Distinct().ToList();
            }
        }
    }
}
