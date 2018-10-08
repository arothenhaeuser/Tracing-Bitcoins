using Aglomera;
using Aglomera.Linkage;
using fd.Coins.Core.Clustering.Intrinsic;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace fd.Coins.Clustering
{
    class Program
    {
        static void Main(string[] args)
        {

            var txgraphOptions = new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" };
            var data = new DataSourceProvider("dbaf14e1c476e76ea05a8b71921a46d6b06f0a950f17c5f9f1a03b8fae467f10", LimitType.DATE, 20160);
            var addresses = data.GetAddresses(txgraphOptions);

            // DEBUG
            Console.WriteLine($"{addresses.Count} addresses of interest will be processed...");

            var algoPipe = new Pipeline();
            algoPipe.Add(new TotalAmounts());
            algoPipe.Add(new TimeSlots());
            algoPipe.Add(new Core.Clustering.Intrinsic.DayOfWeek());
            algoPipe.Add(new Heuristic1());
            algoPipe.Add(new Heuristic2());
            algoPipe.Add(new TransactionShape());


            algoPipe.Process(txgraphOptions, addresses);
            // DEBUG
            Console.WriteLine("Processing of addresses done.");


            // DEBUG
            Console.WriteLine("Clustering...");
            var metric = new AddressDissimilarityMetric(algoPipe);
            var linkage = new AverageLinkage<string>(metric);
            var algorithm = new AgglomerativeClusteringAlgorithm<string>(linkage);

            var clusteringResult = algorithm.GetClustering(new HashSet<string>(addresses));

            foreach(var clusterSet in clusteringResult)
            {
                var sb = new StringBuilder();
                foreach (var cluster in clusterSet)
                {
                    sb.AppendLine(string.Join("\t", cluster));
                }
                Directory.CreateDirectory("report");
                File.WriteAllText(Path.Combine("report", $"Distance_{clusterSet.Dissimilarity}.txt".Replace(",", "#")), sb.ToString());
            }
            // DEBUG
            Console.WriteLine("Clustering done.");

            Console.Read();
        }
    }
}
