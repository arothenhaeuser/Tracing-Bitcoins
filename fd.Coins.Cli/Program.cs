using fd.Coins.Core.NetworkConnector;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace fd.Coins.Cli
{
    /// <summary>
    /// Runs an instance of the BlockProvider to fill database.
    /// </summary>
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
