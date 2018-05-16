using Microsoft.Isam.Esent.Collections.Generic;
using NBitcoin;
using NBitcoin.Protocol;
using Orient.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.Core.NetworkConnector
{
    public class BlockProvider
    {
        private bool _disposed;

        private BitcoinNetworkConnector _network;
        private Node _localClient;

        private int _height;
        private PersistentDictionary<string, string> _utxos { get; set; }

        public int Height
        {
            get
            {
                return _height;
            }
        }
        public TransactionRepository Transactions { get; set; }

        public BlockProvider()
        {
            _network = new BitcoinNetworkConnector();
            if (File.Exists("state.log"))
            {
                _height = int.Parse(File.ReadAllText("state.log"));
            }
            else
            {
                _height = 0;  // genesis block
            }
            _utxos = KeyValueStoreProvider.Instance.GetDatabase("UTXOs");
            Transactions = new TransactionRepository(
                    ConfigurationManager.ConnectionStrings["BitcoinMySQL"].ConnectionString,
                    "transactions");
            Connect();
        }


        private async void PeriodicReport()
        {
            while (!_disposed)
            {
                Console.Clear();
                Console.WriteLine(_height);
                await Task.Delay(5000);
            }
            Console.Clear();
        }

        private async void PeriodicLoadData()
        {
            while (!_disposed)
            {
                // get new blocks
                if (_network.BlockChain.Height > _height)
                {
                    var hashes =
                        _network.BlockChain
                        .ToEnumerable(false)
                        .Skip(_height)
                        .Take(50000)
                        .Select(x => x.HashBlock);
                    try
                    {
                        foreach (var block in _localClient.GetBlocks(hashes))
                        {
                            if (ProcessBlock(block))
                            {
                                _height++;
                            }
                            else
                            {
                                throw new InvalidOperationException($"Processing of block failed. ({block.GetHash()})");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        File.AppendAllText("err.log", DateTime.Now + ":\t" + e.Message + "\n");
                        Save();
                        return;
                    }
                }
                await Task.Delay(60000);
            }
        }
        private bool ProcessBlock(Block block)
        {
            var blockTime = block.Header.BlockTime.LocalDateTime;
            try
            {
                foreach (var tx in block.Transactions)
                {
                    AddVertices(tx, blockTime);
                    AddEdges(tx);
                    MergeVertices();
                }
            }
            catch(Exception e)
            {
                return false;
            }
            return true;
        }
        public void AddVertices(Transaction tx, DateTime blockTime)
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                var current = db.Create.Vertex<OVertex>()
                    .Set("hash", tx.GetHash().ToString())
                    .Set("blockTime", DateTime.Now)
                    .Run();
            }
        }
        public void AddEdges(Transaction tx)
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                for (var i = 0; i < tx.Inputs.Count; i++)
                {
                    db.Create.Edge<OEdge>()
                        .From(db.Create.Vertex<OVertex>().Set("hash", tx.Inputs[i].PrevOut.Hash.ToString()).Run())
                        .To(db.Create.Vertex<OVertex>().Set("hash", tx.GetHash().ToString()).Run())
                        .Set("sTx", tx.Inputs[i].PrevOut.Hash.ToString())
                        .Set("sN", tx.Inputs[i].PrevOut.N)
                        .Set("tTx", tx.GetHash().ToString())
                        .Run();
                }
                for (var i = 0; i < tx.Outputs.Count; i++)
                {
                    db.Create.Edge<OEdge>()
                        .From(db.Create.Vertex<OVertex>().Set("hash", tx.GetHash().ToString()).Run())
                        .To(db.Create.Vertex<OVertex>().Set("hash", "future").Run())
                        .Set("sTx", tx.GetHash().ToString())
                        .Set("sN", i)
                        .Set("tTx", "future")
                        .Set("tAddr", GetAddress(tx.Outputs[i].ScriptPubKey))
                        .Set("amount", tx.Outputs[i].Value.Satoshi)
                        .Set("tTx", "future")
                        .Run();
                }
            }
        }
        public void MergeVertices()
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                var unconnected = db.Query<OVertex>("SELECT FROM V WHERE in().size() = 0 AND out().size() = 0");
                foreach(var uVertex in unconnected)
                {
                    var inEdges = db.Select().From("E").Where("tTx").Equals(uVertex.GetField<string>("hash")).ToList<OEdge>();
                    foreach (var edge in inEdges)
                    {
                        var source = db.Select().From("V").Where("@rid").Equals(edge.GetField<ORID>("out")).ToList<OVertex>().FirstOrDefault();
                        db.Create.Edge<OEdge>()
                            .From(source)
                            .To(uVertex)
                            .Set("sTx", edge.GetField<string>("sTx"))
                            .Set("sN", edge.GetField<int>("sN"))
                            .Set("tTx", uVertex.GetField<string>("hash"))
                            .Run();
                        db.Delete.Edge(edge).Run();
                    }
                    var outEdges = db.Select().From("E").Where("sTx").Equals(uVertex.GetField<string>("hash")).ToList<OEdge>();
                    foreach (var edge in outEdges)
                    {
                        var target = db.Select().From("V").Where("@rid").Equals(edge.GetField<ORID>("in")).ToList<OVertex>().FirstOrDefault();
                        db.Create.Edge<OEdge>()
                            .From(uVertex)
                            .To(target)
                            .Set("sTx", edge.GetField<string>("sTx"))
                            .Set("sN", edge.GetField<int>("sN"))
                            .Set("tTx", target.GetField<string>("hash"))
                            .Set("tAddr", edge.GetField<string>("tAddr"))
                            .Set("amount", edge.GetField<int>("amount"))
                            .Run();
                        db.Delete.Edge(edge).Run();
                    }
                    var twins = db.Select().From("V").Where("hash").Equals(uVertex.GetField<string>("hash")).And("@rid").NotEquals(uVertex.ORID).ToList<OVertex>();
                    foreach (var twin in twins)
                    {
                        db.Delete.Vertex(twin).Run();
                    }
                }
            }
        }
        private bool ProcessNewBlock(Block block)
        {
            var success = true;
            var blockTime = block.Header.BlockTime.LocalDateTime;
            // add outputs to dict
            foreach (var tx in block.Transactions)
            {
                for (var i = 0; i < tx.Outputs.Count; i++)
                {
                    try
                    {
                        _utxos.Add(
                        $"{tx.GetHash().ToString()},{i.ToString()}",
                        new TxOutput(
                            i,
                            tx.GetHash().ToString(),
                            GetAddress(tx.Outputs[i].ScriptPubKey),
                            tx.Outputs[i].Value.Satoshi)
                            .ToString());
                    }
                    catch (Exception e)
                    {
                        if (!(e is ArgumentException))
                        {
                            success = false;
                            File.AppendAllText("err.log", DateTime.Now + ":\t" + e.Message + "\n");
                        }
                    }
                }
            }

            // adding to db
            success = Transactions.AddRange(
                block.Transactions.Select(
                    x => new TransactionEntity(
                        x.GetHash().ToString(),
                        blockTime,
                        ConvertInputs(x),
                        x.Outputs.Select(
                            (z, i) => new TxOutput()
                            {
                                Amount = z.Value.Satoshi,
                                Hash = x.GetHash().ToString(),
                                Position = i,
                                TargetAddress = GetAddress(z.ScriptPubKey)
                            }))).ToList()) && success;
            //clean up UTXOs
            foreach (var key in block.Transactions.Where(x => !x.IsCoinBase).SelectMany(x => x.Inputs.Select(y => $"{y.PrevOut.Hash},{y.PrevOut.N}")).ToList())
            {
                try
                {
                    _utxos.Remove(key);
                }
                catch (Exception e)
                {
                    success = false;
                    File.AppendAllText("err.log", DateTime.Now + ":\t" + e.Message + "\n");
                }
            }
            return success;
        }

        private string GetAddress(Script scriptPubKey)
        {
            var add = scriptPubKey.GetDestinationAddress(Network.Main)?.ToString();
            if (string.IsNullOrEmpty(add))
            {
                var keys = scriptPubKey.GetDestinationPublicKeys();
                add = string.Join(",", keys.Select(x => x.GetAddress(Network.Main)));
            }
            return add;
        }

        private IEnumerable<TxInput> ConvertInputs(Transaction tx)
        {
            var inputs = tx.Inputs;
            if (tx.IsCoinBase)
            {
                return new List<TxInput>() { new TxInput(0, tx.GetHash().ToString(), "coinbase", tx.TotalOut) };
            }
            else
            {
                return inputs.Select((x, i) => new TxInput(tx.GetHash().ToString(), i, _utxos[$"{x.PrevOut.Hash},{x.PrevOut.N}"])); // lookup outputs pointing towards me
            }
        }

        public IEnumerable<Block> GetBlocks(int fromHeight, int toHeight)
        {
            while (_network.CurrentHeight < toHeight)
            {
                Task.Delay(1000);
            }
            var hashes = _network.BlockChain.ToEnumerable(false).Skip(fromHeight).Take(toHeight - fromHeight).Select(x => x.HashBlock);
            return _localClient.GetBlocks(hashes);
        }

        private void Connect()
        {
            _network.Connect();
            _localClient = Node.ConnectToLocal(Network.Main, new NodeConnectionParameters());
            _localClient.VersionHandshake();
        }

        private void Save()
        {
            File.WriteAllText("state.log", _height.ToString());
            _utxos.Flush();
        }

        public void Start()
        {
            PeriodicReport();
            PeriodicLoadData();
        }

        public void Stop()
        {
            _disposed = true;
            _network.Disconnect();
            _localClient.Disconnect();
            Save();
        }
        private bool CreateDatabaseIfNotExists(string hostname, int port, string user, string password, string database)
        {
            using (var server = new OServer(hostname, port, user, password))
            {
                if (!server.DatabaseExist(database, OStorageType.PLocal))
                {
                    return server.CreateDatabase(database, ODatabaseType.Graph, OStorageType.PLocal);
                }
                else
                {
                    return true;
                }
            }
        }
    }
}
