using fd.Coins.Core.NetworkConnector;
using System;
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

            while(Console.ReadLine() != "x")
            {
                Thread.Sleep(1000);
            }

            provider.Stop();
            Console.WriteLine("Stopped.");
            Thread.Sleep(1500);
            

            //var utxos = KeyValueStoreProvider.Instance.GetDatabase("UTXOs");
            //utxos.Add("542c741426f2ed6adcd4bc23f0621f2a0445ae0db9398df193abf08440d2dd64,0", new TxOutput(
            //            "542c741426f2ed6adcd4bc23f0621f2a0445ae0db9398df193abf08440d2dd64",
            //            "1vAtFfxQqZQo4xKi4bjvFYNfAmFth1krr",
            //            1350900000)
            //            .ToString());
            //utxos.Add("542c741426f2ed6adcd4bc23f0621f2a0445ae0db9398df193abf08440d2dd64,1", new TxOutput(
            //            "542c741426f2ed6adcd4bc23f0621f2a0445ae0db9398df193abf08440d2dd64",
            //            "1FHA5QRhxCuJnuvetAu4zjLuitySGJe8kD",
            //            59000000)
            //            .ToString());
            //Console.Read();
        }
    }
}
