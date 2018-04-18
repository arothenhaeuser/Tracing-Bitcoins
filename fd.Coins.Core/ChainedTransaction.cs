using NBitcoin;
using NBitcoin.JsonConverters;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Core
{
    public class ChainedTransaction
    {
        private string _transaction;
        [PrimaryKey]
        public string Hash { get; set; }
        public string[] PrevIds { get; set; }
        public string[] NextIds { get; set; }

        public ChainedTransaction(Transaction tx)
        {
            Hash = tx.GetHash().ToString();
            _transaction = Serializer.ToString(tx);
        }

        public IEnumerable<BitcoinAddress> OutputAddresses()
        {
            return GetTransaction().Outputs.Select(x => x.ScriptPubKey.GetDestinationAddress(Network.Main));
        }

        public IEnumerable<BitcoinAddress> InputAddresses(Dictionary<uint256, Transaction> prevTxs)
        {
            var addresses = new List<BitcoinAddress>();
            foreach (var input in GetTransaction().Inputs)
            {
                Transaction prev = null;
                prevTxs.TryGetValue(input.PrevOut.Hash, out prev);
                if (prev != null)
                {
                    addresses.Add(
                        prev.Outputs[input.PrevOut.N]
                        .ScriptPubKey
                        .GetDestinationAddress(Network.Main));
                }

            }
            return addresses;
        }

        public Transaction GetTransaction()
        {
            return Serializer.ToObject<Transaction>(_transaction);
        }
    }
}
