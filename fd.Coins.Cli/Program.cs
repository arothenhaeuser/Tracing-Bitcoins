using fd.Coins.Core.NetworkConnector;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace fd.Coins.Cli
{
    public struct ConnectionParameters
    {
        string Hostname { get; set; }
        int Port { get; set; }
        string User { get; set; }
        string Password { get; set; }
    }

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
