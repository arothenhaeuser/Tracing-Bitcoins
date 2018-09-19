using NBitcoin;
using NBitcoin.Protocol;
using Orient.Client;
using System;
using System.Collections.Generic;
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
        public int Height
        {
            get
            {
                return _height;
            }
        }

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
            Connect();
        }


        private async void PeriodicReport()
        {
            while (!_disposed)
            {
                Save();
                Console.Clear();
                Console.WriteLine(_height);
                await Task.Delay(10000);
            }
            Console.Clear();
        }
        private bool IsCoinbaseTx(ODocument node)
        {
            return node.GetField<bool>("Coinbase");
        }

        private async void PeriodicLinkData()
        {
            while (!_disposed)
            {
                using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
                {
                    var nodes = db.Select().From("Transaction").Where("Unlinked").Equals(true).Limit(50000).ToList<OVertex>();
                    foreach (var node in nodes)
                    {
                        var transaction = db.Transaction;
                        if (IsCoinbaseTx(node))
                        {
                            node.SetField("Unlinked", false);
                            transaction.AddOrUpdate(node);
                            transaction.Commit();
                            //db.Command($"UPDATE {node.ORID} SET Unlinked = False");
                            continue;
                        }
                        try
                        {
                            transaction.AddOrUpdate(node);
                            for (var i = 0; i < GetInputCount(node); i++)
                            {
                                var inputString = node.GetField<string>($"INPUT{i}");
                                var prevHash = inputString.Split(':')[0];
                                var prevN = Int64.Parse(inputString.Split(':')[1]);
                                var prevTx = db.Query<OVertex>($"SELECT * FROM Transaction WHERE Hash = \"{prevHash}\"").FirstOrDefault();
                                if (prevTx != null)
                                {
                                    transaction.AddOrUpdate(prevTx);
                                    var prevOutString = prevTx.GetField<string>($"OUTPUT{prevN}");
                                    var prevOutN = prevOutString?.Split(':')[0];
                                    var outAddr = prevOutString?.Split(':')[1];
                                    var outAmount = prevOutString != null ? Int64.Parse(prevOutString?.Split(':')[2]) : 0;
                                    var edge = new OEdge() { OClassName = "Link" };
                                    edge.SetField("sTx", prevHash);
                                    edge.SetField("sN", prevN);
                                    edge.SetField("amount", outAmount);
                                    edge.SetField("tTx", node.GetField<string>("Hash"));
                                    edge.SetField("tAddr", outAddr ?? "");
                                    transaction.AddEdge(edge, prevTx, node);
                                }
                            }
                            node.SetField("Unlinked", false);
                            transaction.Update(node);
                            transaction.Commit();
                        }
                        catch (Exception e)
                        {
                            transaction.Reset();
                        }
                    }
                }
                await Task.Delay(60000);
            }
        }

        private int GetInputCount(ODocument node)
        {
            var c = 0;
            var tmp = new object();
            while (node.TryGetValue($"INPUT{c}", out tmp))
            {
                c++;
            }
            return c;
        }

        private int GetOutputCount(ODocument node)
        {
            var c = 0;
            var tmp = new object();
            while (node.TryGetValue($"OUTPUT{c}", out tmp))
            {
                c++;
            }
            return c;
        }

        private async void PeriodicLoadData()
        {
            while (!_disposed)
            {
                // get new blocks
                if (_network.CurrentHeight > _height)
                {
                    var hashes =
                        _network.BlockChain
                        .ToEnumerable(false)
                        .Skip(_height)
                        .Take(50000)
                        .Select(x => x.HashBlock);
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
                await Task.Delay(60000);
            }
        }
        private bool ProcessBlock(Block block)
        {
            var blockTime = block.Header.BlockTime.LocalDateTime;
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                db.DatabaseProperties.ORID = new ORID();
                var tx = db.Transaction;
                try
                {
                    foreach (var transaction in block.Transactions)
                    {
                        var vertex = new OVertex() { OClassName = "Transaction" };
                        vertex.SetField("Hash", transaction.GetHash().ToString());
                        vertex.SetField("BlockTime", blockTime);
                        vertex.SetField("Coinbase", transaction.IsCoinBase);
                        for (var i = 0; i < transaction.Inputs.Count; i++)
                        {
                            var input = transaction.Inputs[i];
                            vertex.SetField($"INPUT{i}", $"{input.PrevOut.Hash}:{input.PrevOut.N}");
                        }
                        for (var i = 0; i < transaction.Outputs.Count; i++)
                        {
                            var output = transaction.Outputs[i];
                            vertex.SetField($"OUTPUT{i}", $"{i}:{GetAddress(output.ScriptPubKey)}:{output.Value.Satoshi}");
                        }
                        tx.Add(vertex);
                    }
                    tx.Commit();
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("ORecordDuplicatedException"))
                        return true; // duplicate tx ids existed in the early days of bitcoin
                    else
                    {
                        tx.Reset();
                        return false;
                    }
                }
            }
            return true;
        }

        public OVertex AddVertex(Transaction tx, DateTime blockTime, ODatabase db)
        {
            var vertex = new OVertex() { OClassName = "Transaction" };
            vertex.SetField("Hash", tx.GetHash().ToString());
            vertex.SetField("BlockTime", blockTime);
            vertex.SetField("Coinbase", tx.IsCoinBase);
            for (var i = 0; i < tx.Inputs.Count; i++)
            {
                var input = tx.Inputs[i];
                vertex.SetField($"INPUT{i}", $"{input.PrevOut.Hash}:{input.PrevOut.N}");
            }
            for (var i = 0; i < tx.Outputs.Count; i++)
            {
                var output = tx.Outputs[i];
                vertex.SetField($"OUTPUT{i}", $"{i}:{GetAddress(output.ScriptPubKey)}:{output.Value.Satoshi}");
            }

            db.Insert(vertex).Run();
            return vertex;
        }

        public string GetAddress(Script scriptPubKey)
        {
            var add = scriptPubKey.GetDestinationAddress(Network.Main)?.ToString();
            if (string.IsNullOrEmpty(add))
            {
                var keys = scriptPubKey.GetDestinationPublicKeys();
                add = string.Join(",", keys.Select(x => x.GetAddress(Network.Main)));
            }
            return add;
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
            try
            {
                _network.Connect();
                _localClient = Node.ConnectToLocal(Network.Main, new NodeConnectionParameters());
                _localClient.VersionHandshake();
            }
            catch (Exception e)
            {
                Error(e);
            }
        }

        private void Save()
        {
            File.WriteAllText("state.log", _height.ToString());
        }

        public void Start()
        {
            CreateDatabaseIfNotExists("localhost", 2424, "root", "root", "txgraph");
            PeriodicReport();
            Task.Run(() =>
            {
                PeriodicLinkData();
            });
            PeriodicLoadData();
        }

        public void Stop()
        {
            _disposed = true;
            if (_network.ConnectedNodes != 0)
            {
                _network.Disconnect();
            }
            if (_localClient.IsConnected)
            {
                _localClient.Disconnect();
            }
            Save();
        }
        private bool CreateDatabaseIfNotExists(string hostname, int port, string user, string password, string database)
        {
            using (var server = new OServer(hostname, port, user, password))
            {
                if (!server.DatabaseExist(database, OStorageType.PLocal))
                {
                    var created = server.CreateDatabase(database, ODatabaseType.Graph, OStorageType.PLocal);
                    using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
                    {
                        db.Command("CREATE CLASS Transaction EXTENDS V");
                        db.Command("CREATE PROPERTY Transaction.Hash STRING");
                        db.Command("CREATE INDEX IndexForHash ON Transaction (Hash) UNIQUE_HASH_INDEX");
                        db.Command("CREATE PROPERTY Transaction.BlockTime DATETIME");
                        db.Command("CREATE PROPERTY Transaction.Coinbase BOOLEAN");
                        db.Command("ALTER PROPERTY Transaction.Coinbase DEFAULT False");
                        db.Command("CREATE PROPERTY Transaction.Unlinked BOOLEAN");
                        db.Command("ALTER PROPERTY Transaction.Unlinked DEFAULT True");
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

        private void Error(Exception e)
        {
            File.AppendAllText("err.log", DateTime.Now + ":\t" + e.ToString() + "\n");
            Stop();
        }
    }
}
