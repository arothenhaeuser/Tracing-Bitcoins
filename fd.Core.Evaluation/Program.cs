using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            var results = new Dictionary<string, List<double>>();
            var evalConf = ReadConfig(@"conf.txt");
            var gold = new List<List<string>>();
            gold = File.ReadAllLines(evalConf.Gold).Select(x => x.Trim().Split('\t').ToList()).ToList();
            var c1 = new List<List<string>>();
            var sw = new Stopwatch();
            sw.Start();
            for (var i = 0; i < evalConf.Clusters.Count(); i++)
            {
                var score = double.Parse(File.ReadAllLines(evalConf.Clusters[i])[0].Replace("Dissimilarity:", ""));
                if(score < 0.53 || score > 0.57)
                {
                    continue;
                }

                c1 = File.ReadAllLines(evalConf.Clusters[i]).Select(x => x.Trim().Split('\t').ToList()).ToList();

                var ri = 0;//Evaluation.RandIndex(gold, c1);
                var ari = Evaluation.AdjustedRandIndex(gold, c1);
                var acc = 0; //Evaluation.Accuracy(gold, c1);
                var pre = 0; //Evaluation.Precision(gold, c1);
                var rec = 0; //Evaluation.Recall(gold, c1);
                var f1 = 0; //Evaluation.F1(gold, c1);

                Console.WriteLine("RandIndex:\t\t" + ri);
                Console.WriteLine("AdjustedRandIndex:\t" + ari);
                Console.WriteLine("Accuracy:\t\t" + acc);
                Console.WriteLine("Precision:\t\t" + pre);
                Console.WriteLine("Recall:\t\t\t" + rec);
                Console.WriteLine("F1:\t\t\t" + f1);

                var dissim = double.Parse(File.ReadAllLines(evalConf.Clusters[i]).First().Replace("Dissimilarity:", ""));
                results.Add(Path.GetFileNameWithoutExtension(evalConf.Clusters[i]), new List<double>() { ri, ari, acc, pre, rec, f1, dissim });
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            var byARIThenRI = results.OrderByDescending(x => x.Value[1]).ThenByDescending(x => x.Value[0]);

            File.WriteAllText("results.txt", string.Join("\n", byARIThenRI.Select(x => $"{x.Key}.txt [Dissim: {x.Value[6]}, ARI:{x.Value[1]}, Acc:{x.Value[2]}, P:{x.Value[3]}, R:{x.Value[4]}, F1:{x.Value[5]}, RI:{x.Value[0]}")));

            Console.Read();
        }

        private static EvalConf ReadConfig(string path)
        {
            var c = new EvalConf();
            var f = File.ReadAllLines(path);
            c.Gold = f.Where(x => x.StartsWith("g:")).First().Replace("g:", "");
            var clusterPath = f.Where(x => x.StartsWith("c:")).First().Replace("c:", "");
            c.Clusters = Directory.GetFiles(clusterPath);
            return c;
        }
    }
}
