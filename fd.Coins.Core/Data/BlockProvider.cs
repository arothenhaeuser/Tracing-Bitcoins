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
            //_utxos = KeyValueStoreProvider.Instance.GetDatabase("UTXOs");
            //Transactions = new TransactionRepository(
            //        ConfigurationManager.ConnectionStrings["BitcoinMySQL"].ConnectionString,
            //        "transactions");
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
                        File.AppendAllText("err.log", DateTime.Now + ":\t" + e.ToString() + "\n");
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
                    var vTx = AddVertex(tx, blockTime);
                    AddEdges(tx, vTx);
                }
            }
            catch(Exception e)
            {
                throw;
            }
            return true;
        }
        public OVertex AddVertex(Transaction tx, DateTime blockTime)
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                return db.Create.Vertex("Transaction")
                    .Set("Hash", tx.GetHash().ToString())
                    .Set("BlockTime", DateTime.Now)
                    .Run();
            }
        }
        public void AddEdges(Transaction tx, OVertex vTx)
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                for (var i = 0; i < tx.Inputs.Count; i++)
                {
                    var source = db.Select().From("Transaction").Where("Hash").Equals(tx.Inputs[i].PrevOut.Hash.ToString()).ToList<OVertex>().FirstOrCoinbase(db, tx.TotalOut.Satoshi);
                    var edge = db.Create.Edge("Link")
                        .From(source)
                        .To(vTx)
                        .Set("sTx", tx.Inputs[i].PrevOut.Hash.ToString())
                        .Set("sN", tx.Inputs[i].PrevOut.N)
                        .Set("tTx", tx.GetHash().ToString())
                        .Run();
                    if (!source.IsCoinBase())
                    {
                        var eFuture = db.Select().From("Link").Where("@rid").In(source.GetField<List<ORID>>("out_Link")).And("sN").Equals(tx.Inputs[i].PrevOut.N).ToList<OEdge>().SingleOrDefault();
                        if (eFuture != null)
                        {
                            edge.SetField("tAddr", eFuture.GetField<string>("tAddr"));
                            edge.SetField("amount", eFuture.GetField<long>("amount"));
                            db.Update(edge).Run();
                            var vFuture = db.Select().From("Transaction").Where("@rid").Equals(eFuture.InV).ToList<OVertex>().SingleOrDefault();
                            if (vFuture != null)
                            {
                                db.Delete.Vertex(vFuture).Run();
                            }
                        }
                    }
                }
                for (int i = 0; i < tx.Outputs.Count; i++)
                {
                    db.Create.Edge("Link")
                        .From(vTx)
                        .To(db.Create.Vertex("Transaction").Set("Hash", "future").Run())
                        .Set("sTx", tx.GetHash().ToString())
                        .Set<long>("sN", i)
                        .Set("tTx", "future")
                        .Set("tAddr", GetAddress(tx.Outputs[i].ScriptPubKey))
                        .Set("amount", tx.Outputs[i].Value.Satoshi)
                        .Run();
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
            CreateDatabaseIfNotExists("localhost", 2424, "root", "root", "txgraph");
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
                //debug
                if(server.DatabaseExist(database, OStorageType.PLocal))
                    server.DropDatabase(database, OStorageType.PLocal);
                File.Delete("err.log");
                File.Delete("state.log");
                if (!server.DatabaseExist(database, OStorageType.PLocal))
                {
                    var created = server.CreateDatabase(database, ODatabaseType.Graph, OStorageType.PLocal);
                    using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
                    {
                        db.Command("CREATE CLASS Transaction EXTENDS V");
                        db.Command("CREATE PROPERTY Transaction.Hash STRING");
                        db.Command("CREATE PROPERTY Transaction.BlockTime DATETIME");
                        db.Command("CREATE CLASS Link EXTENDS E");
                        db.Command("CREATE PROPERTY Link.sTx STRING");
                        db.Command("CREATE PROPERTY Link.sN LONG");
                        db.Command("CREATE PROPERTY Link.tTx STRING");
                        db.Command("CREATE PROPERTY Link.tAddr STRING");
                        db.Command("CREATE PROPERTY Link.amount LONG");
                    }
                    return created;
                }
                else
                {
                    return true;
                }
            }
        }
    }
}
