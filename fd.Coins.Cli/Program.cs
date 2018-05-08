using fd.Coins.Core.NetworkConnector;
using Orient.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
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

        public static bool CreateDatabaseIfNotExists(string hostname, int port, string user, string password, string database)
        {
            using(var server = new OServer(hostname, port, user, password))
            {
                if(!server.DatabaseExist(database, OStorageType.Memory))
                {
                    return server.CreateDatabase(database, ODatabaseType.Graph, OStorageType.Memory);
                }
                else
                {
                    return true;
                }
            }
        }
        public static long CreateUserGraph(string hostname, int port, string user, string password, string database, IEnumerable<TransactionEntity> txs)
        {
            using(var db = new ODatabase(hostname, port, database, ODatabaseType.Graph, user, password))
            {
                // create nodes
                foreach (var tx in txs)
                {
                    tx.Inputs.ForEach(x => db.Create.Vertex<OVertex>().Set("address", x.SourceAddress).Run());
                    tx.Outputs.ForEach(x => db.Create.Vertex<OVertex>().Set("address", x.TargetAddress).Run());
                }
                return db.CountRecords;
            }
        }
        static void Main(string[] args)
        {
            Console.WriteLine("(L)oad or (G)raph?");
            var decision = Console.ReadLine();
            if (decision == "G" || decision == "g")
            {
                if(CreateDatabaseIfNotExists("localhost", 2424, "root", "root", "usergraph"))
                {
                    var transactions = new TransactionRepository(
                        ConfigurationManager.ConnectionStrings["BitcoinMySQL"].ConnectionString,
                        "transactions");
                    Console.WriteLine(CreateUserGraph("localhost", 2424, "admin", "admin", "usergraph", transactions.GetAll()));
                }
                Console.Read();
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
