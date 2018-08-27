using fd.Coins.Core.Clustering.Intrinsic;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Linq;

namespace fd.Coins.Clustering
{
    class Program
    {
        static void Main(string[] args)
        {
            var txgraphOptions = new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" };

            using (var txgraph = new ODatabase(txgraphOptions))
            {
                var totalAmounts = new TotalAmounts();
                var timeSlots = new TimeSlots();
                var h1 = new Heuristic1();
                var h2 = new Heuristic2();

                long skip = 0;
                long limit = 5000;
                long total = txgraph.CountRecords;

                while (skip < 5000)
                {
                    var rids = txgraph.Command("SELECT @rid FROM Transaction WHERE @rid > #13:10570000 LIMIT 10000"/*$"SELECT @rid FROM Transaction SKIP {skip} LIMIT {limit}"*/).ToList().Select(x => x.GetField<ORID>("rid")).ToList();
                    skip += limit;
                    totalAmounts.Run(txgraphOptions, rids);
                    timeSlots.Run(txgraphOptions, rids);
                    h1.Run(txgraphOptions, rids);
                    h2.Run(txgraphOptions, rids);
                    Console.WriteLine($"{skip} processed.");
                }
                totalAmounts.ToFile("report");
                timeSlots.ToFile("report");
                h1.ToFile("report");
                h2.ToFile("report");
            }
        }
    }
}
