using fd.Coins.Core.Clustering.Intrinsic;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Linq;
using System.Threading.Tasks;

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
                var dayOfWeek = new Core.Clustering.Intrinsic.DayOfWeek();
                //var h1 = new Heuristic1();
                //var h2 = new Heuristic2();

                long skip = 3772550;
                long limit = 5000;
                long total = txgraph.CountRecords;

                while (skip < 3772551/*total*/)
                {
                    var rids = txgraph.Command($"SELECT @rid FROM Transaction SKIP {skip} LIMIT {limit}").ToList().Select(x => x.GetField<ORID>("rid")).ToList();
                    skip += limit;
                    var tTotalAmounts = Task.Run(() =>
                    {
                        totalAmounts.Run(txgraphOptions, rids);
                    });
                    var tTimeSlots = Task.Run(() =>
                    {
                        timeSlots.Run(txgraphOptions, rids);
                    });
                    var tDayOfWeek = Task.Run(() =>
                    {
                        dayOfWeek.Run(txgraphOptions, rids);
                    });
                    //var tH1 = Task.Run(() => {
                    //    h1.Run(txgraphOptions, rids);
                    //});
                    //var tH2 = Task.Run(() => {
                    //    h2.Run(txgraphOptions, rids);
                    //});
                    Task.WaitAll(tTotalAmounts, tTimeSlots, tDayOfWeek/*, tH1, tH2*/);
                    Console.WriteLine($"{skip - 3772550 } processed.");
                }
                totalAmounts.ToFile("report");
                timeSlots.ToFile("report");
                dayOfWeek.ToFile("report");
                //h1.ToFileChained("report");
                //h2.ToFileChained("report");
            }

            Console.Read();
        }
    }
}
