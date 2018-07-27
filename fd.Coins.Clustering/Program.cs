using fd.Coins.Core.Clustering.Intrinsic;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fd.Coins.Clustering
{
    class Program
    {
        static void Main(string[] args)
        {
            //var timeSlots = new TimeSlots();
            //timeSlots.Run(new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" });
            //timeSlots.ToFile("report");
            var txgraphOptions = new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" };
            using (var txgraph = new ODatabase(txgraphOptions))
            {
                var totalAmounts = new TotalAmounts();
                long skip = 0;
                long limit = 50000;
                long total = txgraph.CountRecords;
                while(skip < total)
                {
                    var records = txgraph.Command($"SELECT @rid FROM Transaction SKIP {skip} LIMIT {limit}").ToList();
                    skip += limit;
                    totalAmounts.Run(txgraphOptions, records.Select(x => x.GetField<ORID>("rid")));
                    Console.WriteLine($"{skip} processed.");
                }
                totalAmounts.ToFile("report");
            }
        }
    }
}
