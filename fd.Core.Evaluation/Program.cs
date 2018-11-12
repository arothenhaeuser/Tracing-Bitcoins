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
            var results = new Dictionary<double, List<double>>();
            var evalConf = ReadConfig(@"conf.txt");
            var gold = new List<List<string>>();
            gold = File.ReadAllLines(evalConf.Gold).Select(x => x.Trim().Split('\t').ToList()).ToList();
            var c1 = new List<List<string>>();
            var sw = new Stopwatch();
            sw.Start();
            for(var i = 0; i < evalConf.Clusters.Count(); i++)
            {
                c1 = File.ReadAllLines(evalConf.Clusters[i]).Select(x => x.Trim().Split('\t').ToList()).ToList();

                var ri = Evaluation.RandIndex(gold, c1);
                var ari = Evaluation.AdjustedRandIndex(gold, c1);
                var acc = Evaluation.Accuracy(gold, c1);
                var pre = Evaluation.Precision(gold, c1);
                var rec = Evaluation.Recall(gold, c1);
                var f1 = Evaluation.F1(gold, c1);

                Console.WriteLine("RandIndex:\t\t" + ri);
                Console.WriteLine("AdjustedRandIndex:\t" + ari);
                Console.WriteLine("Accuracy:\t\t" + acc);
                Console.WriteLine("Precision:\t\t" + pre);
                Console.WriteLine("Recall:\t\t\t" + rec);
                Console.WriteLine("F1:\t\t\t" + f1);

                results.Add(double.Parse(Path.GetFileNameWithoutExtension(evalConf.Clusters[i])), new List<double>() { ri, ari, acc, pre, rec, f1 });
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            var byARIThenRI = results.OrderByDescending(x => x.Value[1]).ThenByDescending(x => x.Value[0]);

            File.WriteAllText("results.txt", string.Join("\n", byARIThenRI.Select(x => $"{x.Key}.txt [RI:{x.Value[0]}, ARI:{x.Value[1]}, Acc:{x.Value[2]}, P:{x.Value[3]}, R:{x.Value[4]}, F1:{x.Value[5]}")));

            Console.Read();
        }

        private static EvalConf ReadConfig(string path)
        {
            var c = new EvalConf();
            var f = File.ReadAllLines(path);
            c.Gold = f.Where(x => x.StartsWith("g:")).First().Replace("g:", "");
            c.Clusters = f.Where(x => x.StartsWith("c:")).Select(x => x.Replace("c:", "")).ToArray();
            return c;
        }
    }
}
