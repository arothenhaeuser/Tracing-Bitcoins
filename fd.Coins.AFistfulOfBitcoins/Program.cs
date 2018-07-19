using Orient.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.AFistfulOfBitcoins
{
    class Program
    {
        static void Main(string[] args)
        {
            Utils.ResetPreviousRun();

            using (var txdb = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "root", "root"))
            using (var addrdb = new ODatabase("localhost", 2424, "addressclusters", ODatabaseType.Graph, "admin", "admin"))
            {
                var h1 = new Heuristic1();
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                h1.Run();
                stopwatch.Stop();
                Console.WriteLine($"Heuristic 1 took {stopwatch.Elapsed}.");
                
                var stopwatch2 = new Stopwatch();
                var h2 = new Heuristic2();
                stopwatch2.Start();
                h2.Run();
                stopwatch2.Stop();
                Console.WriteLine($"Heuristic 2 took {stopwatch2.Elapsed}.");

                // get the root of each connected component in the graph
                var roots = addrdb.Command("SELECT distinct(traversedElement(0)) AS root FROM (TRAVERSE * FROM V)").ToList().Select(x => x.GetField<ORID>("root"));
                // traverse from each root to get addresses of each connected component as list
                var addrClusters = new List<IEnumerable<string>>();
                foreach (var root in roots)
                {
                    var cluster = addrdb.Command($"TRAVERSE * FROM {root.RID}").ToList().Select(x => x.GetField<string>("Address")).Where(x => !string.IsNullOrEmpty(x));
                    if (cluster.Count() > 1)
                        addrClusters.Add(cluster);
                }
                File.WriteAllLines("Clusters.txt", addrClusters.Select(x => string.Join("\t", x)));
                Console.WriteLine($"{addrClusters.Count} clusters detected, containing {addrClusters.Sum(x => x.Count())} addresses.");
                Console.WriteLine("Finished");
                Console.ReadLine();
            }
        }
    }
}
