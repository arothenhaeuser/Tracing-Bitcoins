using fd.Coins.Core.NetworkConnector;
using System;

namespace fd.Coins.Cli
{
    class Program
    {
        static void Main(string[] args)
        {
            var provider = new BlockProvider();
            provider.Start();
            Console.Read();
            provider.Stop();
        }
    }
}
