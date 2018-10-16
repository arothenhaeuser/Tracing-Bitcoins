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
            var evalConf = ReadConfig(@"conf.txt");
            var gold = new List<List<string>>();
            gold = File.ReadAllLines(evalConf.Gold).Select(x => x.Trim().Split('\t').ToList()).ToList();
            var c1 = new List<List<string>>();
            var sw = new Stopwatch();
            sw.Start();
            for(var i = 0; i < evalConf.Clusters.Count(); i++)
            {
                c1 = File.ReadAllLines(evalConf.Clusters[i]).Select(x => x.Trim().Split('\t').ToList()).ToList();

                Console.WriteLine("RandIndex:\t\t" + Evaluation.RandIndex(gold, c1));
                Console.WriteLine("AdjustedRandIndex:\t" + Evaluation.AdjustedRandIndex(gold, c1));
                Console.WriteLine("Accuracy:\t\t" + Evaluation.Accuracy(gold, c1));
                Console.WriteLine("Precision:\t\t" + Evaluation.Precision(gold, c1));
                Console.WriteLine("Recall:\t\t\t" + Evaluation.Recall(gold, c1));
                Console.WriteLine("F1:\t\t\t" + Evaluation.F1(gold, c1));
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

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
