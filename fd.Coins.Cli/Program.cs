using fd.Coins.Core.NetworkConnector;
using Orient.Client;
using System;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace fd.Coins.Cli
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("(L)oad or (G)raph?");
            var decision = Console.ReadLine();
            if (decision == "G" || decision == "g")
                {
                using (var server = new OServer("localhost", 2424, "root", "root"))
                {
                    if (!server.DatabaseExist("test", OStorageType.Memory))
                    {
                        var success = server.CreateDatabase("test", ODatabaseType.Graph, OStorageType.Memory);
                        switch (success)
                        {
                            case true:
                                Console.WriteLine("Succeeded!");
                                break;
                            case false:
                                Console.WriteLine("Failed!");
                                break;
                            default:
                                break;
                        }
                    }
                }
                using (var database = new ODatabase("localhost", 2424, "test", ODatabaseType.Graph, "admin", "admin"))
                {
                    var Transactions = new TransactionRepository(
                        ConfigurationManager.ConnectionStrings["BitcoinMySQL"].ConnectionString,
                        "transactions");
                    foreach (var tx in Transactions.GetAll())
                    {
                        OVertex[] sources = tx.Inputs.Select(x => database.Create.Vertex<OVertex>().Set("address", x.SourceAddress).Run()).ToArray();
                        OVertex[] targets = tx.Outputs.Select(x => database.Create.Vertex<OVertex>().Set("address", x.TargetAddress).Run()).ToArray();

                        foreach (var source in sources)
                        {
                            foreach (var target in targets)
                            {
                                database.Create.Edge<OEdge>().From(source).To(target).Run();
                            }
                        }
                    }
                }
            }
            else if (decision == "L" || decision == "l")
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
}
