﻿using Aglomera;
using Aglomera.Linkage;
using fd.Coins.Core.Clustering.FeatureExtractors;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace fd.Coins.Clustering
{
    class Program
    {
        static void Main(string[] args)
        {
            var txgraphOptions = new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" };
            //var data = new DataSourceProvider("dbaf14e1c476e76ea05a8b71921a46d6b06f0a950f17c5f9f1a03b8fae467f10", LimitType.DATE, 1);
            //var addresses = data.GetAddresses(txgraphOptions);
            //if(addresses.Count == 0)
            //{
            //    return;
            //}
            // DEBUG
            var addresses = ReadGold();
            // DEBUG
            Console.WriteLine($"{addresses.Count} addresses of interest will be processed...");

            var algoPipe = new Pipeline();
            algoPipe.Add(new TotalAmounts());
            algoPipe.Add(new Amounts());
            algoPipe.Add(new SocialNetwork());
            algoPipe.Add(new TimeSlots());
            algoPipe.Add(new Core.Clustering.FeatureExtractors.DayOfWeek());
            algoPipe.Add(new TransactionShape());
            algoPipe.Add(new CommonTimes());
            algoPipe.Add(new Heuristic1());
            algoPipe.Add(new Heuristic2());

            algoPipe.Process(txgraphOptions, addresses);
            // DEBUG
            Console.WriteLine("Processing of addresses done.");

            var metric = new AddressDissimilarityMetric(algoPipe, addresses);
            var linkage = new AverageLinkage<string>(metric);
            var algorithm = new AgglomerativeClusteringAlgorithm<string>(linkage);

            var clusteringResult = algorithm.GetClustering(new HashSet<string>(addresses));
            var index = 0;
            foreach (var clusterSet in clusteringResult.OrderBy(x => x.Dissimilarity))
            {
                var sb = new StringBuilder();
                sb.AppendLine($"{clusterSet.Dissimilarity}");
                foreach (var cluster in clusterSet)
                {
                    sb.AppendLine(string.Join("\t", cluster));
                }
                Directory.CreateDirectory("report");
                File.WriteAllText(Path.Combine("report", $"{index++}.txt".Replace(",", "#")), sb.ToString());
            }
        }

        private static List<string> ReadGold()
        {
            var file = File.ReadAllLines(@"D:\Source\Repos\CurrentVersion\shuffled_cleaned_gold (sarah thibault misc tags).txt").Take(130);
            return file.SelectMany(x => x.Split('\t')).Distinct().ToList();
        }
    }
}
