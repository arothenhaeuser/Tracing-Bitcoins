using System;
using System.Collections.Generic;

namespace fd.Coins.Core.NetworkConnector
{
    public class TransactionEntity
    {
        public TransactionEntity()
        {
            Inputs = new List<TxInput>();
            Outputs = new List<TxOutput>();
        }
        public TransactionEntity(string hash, string blockTime, IEnumerable<TxInput> inputs, IEnumerable<TxOutput> outputs, byte[] payload)
        {
            Hash = hash;
            BlockTime = blockTime;
            Inputs = new List<TxInput>(inputs);
            Outputs = new List<TxOutput>(outputs);
            Payload = payload;
        }

        public string Hash { get; set; }
        public string BlockTime { get; set; }
        public List<TxInput> Inputs { get; set; }
        public List<TxOutput> Outputs { get; set; }
        public byte[] Payload { get; set; }
    }
}
