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
            var data = new DataSourceProvider("8ac3e3c9c9ebbf454f6996f4fee35db6c431a931200f453340f9d471b3223e1b", LimitType.COUNT, 10);
            var addresses = data.GetAddresses(txgraphOptions);

            var algoPipe = new List<Core.Clustering.Clustering>();
            algoPipe.Add(new TotalAmounts());
            algoPipe.Add(new TimeSlots());
            algoPipe.Add(new Core.Clustering.Intrinsic.DayOfWeek());
            //algoPipe.Add(new Heuristic1());
            //algoPipe.Add(new Heuristic2());


            //using (var txgraph = new ODatabase(txgraphOptions))
            //{
            //    long skip = 0;
            //    long limit = 50000;
            //    long total = txgraph.CountRecords;

            //    while (skip < 1000000/*total*/)
            //    {
            //        var rids = txgraph.Command($"SELECT @rid FROM Transaction SKIP {skip} LIMIT {limit}").ToList().Select(x => x.GetField<ORID>("rid")).ToList();
            //        skip += limit;

            var tasks = new List<Task>();
            foreach (var algo in algoPipe)
            {
                tasks.Add(Task.Run(() =>
                {
                    algo.Run(txgraphOptions, addresses);
                }));
            }
            Task.WaitAll(tasks.ToArray());
            //Console.WriteLine($"{skip} processed.");
            //}

            var result = new Dictionary<string, double>();
            foreach (var addr in addresses)
            {
                var distance = 0.0;
                foreach (var algo in algoPipe)
                {
                    distance += algo.Distance(addr, addresses[0]);
                }
                distance /= algoPipe.Count;
                result.Add(addr, distance);
            }
            Console.WriteLine(string.Join("\n", result.OrderBy(x => x.Value).Select(x => string.Join(",", x.Key, x.Value))));
            //}

            var debug = result.OrderBy(x => x.Value).ToDictionary(x => x.Key, y => y.Value);

            Console.Read();
        }
    }
}
