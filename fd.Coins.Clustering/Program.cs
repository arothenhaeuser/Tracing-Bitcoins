using Aglomera;
using Aglomera.Linkage;
using fd.Coins.Core.Clustering.Intrinsic;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;

namespace fd.Coins.Clustering
{
    class Program
    {
        static void Main(string[] args)
        {

            var txgraphOptions = new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" };
            var data = new DataSourceProvider("deca4025c0ccc492b775362f73b4e6c572dbb56a72d1df0433473d2e5ff8ec91", LimitType.DEPTH, 2);
            var addresses = data.GetAddresses(txgraphOptions);
            // DEBUG
            Console.WriteLine($"{addresses.Count} addresses of interest will be processed...");

            var algoPipe = new Pipeline();
            algoPipe.Add(new TotalAmounts());
            algoPipe.Add(new TimeSlots());
            algoPipe.Add(new Core.Clustering.Intrinsic.DayOfWeek());
            algoPipe.Add(new Heuristic1());
            algoPipe.Add(new Heuristic2());


            algoPipe.Process(txgraphOptions, addresses);
            // DEBUG
            Console.WriteLine("Processing of addresses done.");


            // DEBUG
            Console.WriteLine("Clustering...");
            var metric = new AddressDissimilarityMetric(algoPipe);
            var linkage = new AverageLinkage<string>(metric);
            var algorithm = new AgglomerativeClusteringAlgorithm<string>(linkage);

            var clusteringResult = algorithm.GetClustering(new HashSet<string>(addresses));
            //}

            foreach(var clusterSet in clusteringResult)
            {
                Console.WriteLine($"########{clusterSet.Dissimilarity}########");
                foreach(var cluster in clusterSet)
                {
                    Console.WriteLine(string.Join("\t", cluster));
                }
                Console.WriteLine("############");
            }

            Console.Read();
        }
    }
}
