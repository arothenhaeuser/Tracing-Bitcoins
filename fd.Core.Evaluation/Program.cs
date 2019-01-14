using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace fd.Coins.Evaluation
{
    class EvalConf
    {
        public string Gold { get; set; }
        public string[] Clusters { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            var results = new Dictionary<string, double>();

            var evalConf = ReadConfig(@"conf.txt");

            var gold = new List<List<string>>();
            gold = File.ReadAllLines(evalConf.Gold).Select(x => x.Trim().Split('\t').ToList()).ToList();

            var c1 = new List<List<string>>();
            for (var i = 0; i < evalConf.Clusters.Count(); i++)
            {
                c1 = File.ReadAllLines(evalConf.Clusters[i]).Select(x => x.Trim().Split('\t').ToList()).ToList();

                var ari = Evaluation.AdjustedRandIndex(gold, c1);

                Console.WriteLine("AdjustedRandIndex:\t" + ari);

                var dissim = double.Parse(File.ReadAllLines(evalConf.Clusters[i]).First().Replace("Dissimilarity:", ""));
                results.Add(Path.GetFileNameWithoutExtension(evalConf.Clusters[i]), ari);
            }

            var byARI = results.OrderByDescending(x => x.Value);

            File.WriteAllText("results.txt", string.Join("\n", byARI.Select(x => $"{x.Key}.txt [ARI:{x.Value}]")));

            Console.Read();
        }

        private static EvalConf ReadConfig(string path)
        {
            var c = new EvalConf();
            var f = File.ReadAllLines(path);
            c.Gold = f.Where(x => x.StartsWith("g:")).First().Replace("g:", "");
            var clusterPath = f.Where(x => x.StartsWith("c:")).First().Replace("c:", "");
            c.Clusters = Directory.GetFiles(clusterPath).Where(x => Path.GetFileNameWithoutExtension(x) != "results").ToArray();
            return c;
        }
    }
}
