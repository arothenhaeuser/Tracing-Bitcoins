using Coins.Core;
using fd.Coins.Core;
using Microsoft.Isam.Esent.Collections.Generic;
using NBitcoin;
using NBitcoin.JsonConverters;
using NBitcoin.Protocol;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace fd.Coins
{
    public class BlockProvider
    {
        public static readonly string[] KNOWN_DUPLICATE_TRANSACTION_HASHES = new string[]
        {
            "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599",
            "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468"
        };

        private bool _disposed;

        private BitcoinNetworkConnector _network;
        private Node _localClient;

        private int _height;
        public TransactionRepository Repo { get; set; }
        public PersistentDictionary<string, string> UTXOs { get; set; }
        //public PersistentDictionary<string, string> PrevOutPoints { get; set; }
        //public PersistentDictionary<string, string> NextInPoints { get; set; }

        public BlockProvider()
        {
            _network = new BitcoinNetworkConnector();
            if (File.Exists("log.txt"))
            {
                _height = int.Parse(File.ReadAllText("log.txt"));
            }
            else
            {
                _height = 0; // genesis block
            }
            Repo = new TransactionRepository(
                    ConfigurationManager.ConnectionStrings["BitcoinMySQL"].ConnectionString,
                    "test");
            UTXOs = DatabaseConnectionProvider.Instance.GetDatabase("UTXOs");
            //PrevOutPoints = DatabaseConnectionProvider.Instance.GetDatabase("PrevOut");
            //NextInPoints = DatabaseConnectionProvider.Instance.GetDatabase("NextIn");
        }

        private void Save()
        {
            File.WriteAllText("log.txt", _height.ToString());
        }

        private async void PeriodicReport()
        {

            while (!_disposed)
            {
                Console.Clear();
                Console.WriteLine(_height);
                Save();
                await Task.Delay(2000);
            }
        }

        private async void PeriodicLoadData()
        {
            while (!_disposed)
            {
                // Get new blocks
                if (_network.BlockChain.Height > _height)
                {
                    var h = _network.BlockChain.ToEnumerable(false).Skip(_height).Take(50000).Select(x => x.HashBlock);

                    foreach (var block in _localClient.GetBlocks(h))
                    {
                        ProcessNewBlock(block);
                        _height++;
                    }
                }
                await Task.Delay(60000);
            }
        }

        private void ProcessNewBlock(Block block)
        {
            var blockTime = block.Header.BlockTime.ToString("yyyy-MM-dd hh:mm:ss");
            // add outputs to dict
            foreach (var tx in block.Transactions)
            {
                for (int i = 0; i < tx.Outputs.Count; i++)
                {
                    try
                    {
                        UTXOs.AddOrReplace(
                        $"{tx.GetHash().ToString()},{i.ToString()}",
                        new TxOutput(
                            tx.GetHash().ToString(),
                            GetAddress(tx.Outputs[i].ScriptPubKey),
                            tx.Outputs[i].Value.Satoshi)
                            .ToString());
                    }
                    catch (ArgumentException ae)
                    {
                        if (!KNOWN_DUPLICATE_TRANSACTION_HASHES.Contains($"{tx.GetHash().ToString()}"))
                        {
                            throw ae;
                        }
                    }
                }
            }
            // lookup outputs pointing towards me
            // adding to db
            if(!Repo.AddRange(
                block.Transactions.Select(
                    x => new TransactionEntity(
                        x.GetHash().ToString(),
                        blockTime,
                        ConvertInputs(x),
                        x.Outputs.Select(
                            z => new TxOutput(
                                x.GetHash().ToString(),
                                GetAddress(z.ScriptPubKey),
                                z.Value.Satoshi)),
                        x.ToBytes()))))
            {
                Debug.WriteLine(block.ToString());
            }
            //clean up UTXOs
            foreach (var key in block.Transactions.Where(x => !x.IsCoinBase).SelectMany(x => x.Inputs.Select(y => $"{y.PrevOut.Hash},{y.PrevOut.N}")))
            {
                UTXOs.Remove(key);
            }
            //Repo.AddRange(
            //    block.Transactions.Select(
            //        x => new TransactionEntity(x.GetHash().ToString(), block.Header.BlockTime.ToString("yyyy-MM-dd hh:mm:ss"), GetInputs(x), GetOutputs(x), x.ToBytes())));

            //var txEs = new List<TransactionEntity>();
            //foreach (var tx in block.Transactions)
            //{
            //    if (!tx.IsCoinBase)
            //    {
            //        var inputs = GetInputs(tx);
            //        var outputs = GetOutputs(tx);
            //        txEs.Add(new TransactionEntity(tx.GetHash().ToString(), blockTime, inputs, outputs, tx.ToBytes()));
            //    }

            //    var txEntity = new TransactionEntity(tx.GetHash().ToString(), block.Header.BlockTime.ToString("yyyy-MM-dd hh:mm:ss"), inputs, outputs, tx.ToBytes());
            //    Repo.Add(txEntity);
            //    //if (!Txs.ContainsKey(tx.GetHash().ToString()))
            //    //{
            //    //    // Store data to db
            //    //    Txs.TryAdd(tx.GetHash().ToString(), Serializer.ToString(tx));

            //    //    // Chain the txs
            //    //    if (!PrevOutPoints.ContainsKey(tx.GetHash().ToString()))
            //    //    {
            //    //        PrevOutPoints.Add(tx.GetHash().ToString(), Serializer.ToString(tx.Inputs.Select(x => x.PrevOut).ToList()));
            //    //    }
            //    //    foreach (var input in tx.Inputs.AsIndexedInputs())
            //    //    {
            //    //        if (NextInPoints.ContainsKey(input.PrevOut.Hash.ToString()))
            //    //        {
            //    //            var list = Serializer.ToObject<List<InPoint>>(NextInPoints[input.PrevOut.Hash.ToString()]);
            //    //            list.Add(new InPoint(tx.GetHash(), input.Index));
            //    //            NextInPoints[input.PrevOut.Hash.ToString()] = Serializer.ToString(list);
            //    //        }
            //    //        else
            //    //        {
            //    //            NextInPoints.Add(input.PrevOut.Hash.ToString(), Serializer.ToString(new List<InPoint>() { new InPoint(tx.GetHash(), input.Index) }));
            //    //        }
            //    //    }
            //    //}
            //}
        }
        private IEnumerable<TxInput> ConvertInputs(Transaction tx)
        {
            if (tx.IsCoinBase)
            {
                return new List<TxInput>() { new TxInput(tx.GetHash().ToString(), "coinbase", tx.TotalOut) };
            }
            else
            {
                return tx.Inputs.Select(x => new TxInput(tx.GetHash().ToString(), UTXOs[$"{x.PrevOut.Hash},{x.PrevOut.N}"]));
            }
        }

        private IEnumerable<TxInput> GetInputs(Transaction tx)
        {
            var inputs = new List<TxInput>();

            if (!tx.IsCoinBase)
            {
                foreach (var input in tx.Inputs)
                {
                    var prevTx = new Transaction();
                    var prevTxEntity = Repo.GetById(input.PrevOut.Hash.ToString());
                    if (prevTxEntity != null)
                    {
                        prevTx.FromBytes(prevTxEntity.Payload);

                        var prevOut = prevTx.Outputs[input.PrevOut.N];
                        var sourceAddress = GetAddress(prevOut.ScriptPubKey);

                        inputs.Add(new TxInput(tx.GetHash().ToString(), sourceAddress, prevOut.Value.Satoshi));
                    }
                }
            }
            return inputs;
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

        private IEnumerable<TxOutput> GetOutputs(Transaction tx)
        {
            var outputs = new List<TxOutput>();

            foreach (var output in tx.Outputs)
            {
                var tmp = new TxOutput(tx.GetHash().ToString(), GetAddress(output.ScriptPubKey), output.Value.Satoshi);
                outputs.Add(tmp);
            }
            return outputs;
        }

        public IEnumerable<Block> GetBlocks(int from, int to)
        {
            _network.Connect();
            Thread.Sleep(30000);
            _localClient = Node.ConnectToLocal(Network.Main, new NodeConnectionParameters());
            _localClient.VersionHandshake();
            var hashes = _network.BlockChain.ToEnumerable(false).Select(x => x.HashBlock).Skip(from).Take(to - from);
            return _localClient.GetBlocks(hashes);
        }

        public void Start()
        {
            _network.Connect();
            _localClient = Node.ConnectToLocal(Network.Main, new NodeConnectionParameters());
            _localClient.VersionHandshake();
            PeriodicReport();
            PeriodicLoadData();
        }

        public void Stop()
        {
            _network.Disconnect();
            _localClient.Disconnect();
            Save();
            _disposed = true;
        }
    }
}
