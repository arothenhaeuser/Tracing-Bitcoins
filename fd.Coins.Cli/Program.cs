using fd.Coins.Core.NetworkConnector;
using NBitcoin;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace fd.Coins.Cli
{
    class Program
    {
        static void Main(string[] args)
        {
            var provider = new BlockProvider();
            Task.Run(() =>
            {
                provider.Start();
            });

            while (Console.ReadLine() != "x")
            {
                Thread.Sleep(1000);
            }

            provider.Stop();
            Console.WriteLine("Stopped.");
            Thread.Sleep(1500);
        }
    }
}
