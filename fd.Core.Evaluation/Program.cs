using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fd.Coins.Evaluation
{
    class Program
    {
        static void Main(string[] args)
        {
            var gold = new List<List<string>>();
            gold = File.ReadAllLines(@"D:\Source\Repos\CurrentVersion\fd.Coins.AFistfulOfBitcoins\bin\Debug\Clusters.txt").Select(x => x.Split('\t').ToList()).ToList();
            var c1 = new List<List<string>>();
            c1 = File.ReadAllLines(@"D:\Source\Repos\CurrentVersion\fd.Coins.AFistfulOfBitcoins\bin\Debug\Clusters - Kopie.txt").Select(x => x.Split('\t').ToList()).ToList();

            Console.WriteLine("RandIndex:\t\t" + Evaluation.RandIndex(c1, gold));
            Console.WriteLine("AdjustedRandIndex:\t" + Evaluation.AdjustedRandIndex(c1, gold));
            Console.WriteLine("Accuracy:\t\t" + Evaluation.Accuracy(c1, gold));
            Console.WriteLine("Precision:\t\t" + Evaluation.Precision(c1, gold));
            Console.WriteLine("Recall:\t\t\t" + Evaluation.Recall(c1, gold));
            Console.WriteLine("F1:\t\t\t" + Evaluation.F1(c1, gold));

            Console.Read();
        }
    }
}
