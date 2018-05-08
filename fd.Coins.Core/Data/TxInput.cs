using System;

namespace fd.Coins.Core.NetworkConnector
{
    public class TxInput
    {
        //public TxInput() { }
        public TxInput(string hash, int position, string prevOut)
        {
            try
            {
                var components = prevOut.Split(';');
                Hash = hash;
                Position = position;
                SourceAddress = components[2];
                Amount = long.Parse(components[3]);
            }
            catch
            {
                throw new ArgumentException("The string supplied is invalid.");
            }
        }
        public TxInput(int position, string hash, string sourceAddress, long amount)
        {
            Position = position;
            Hash = hash;
            SourceAddress = sourceAddress;
            Amount = amount;
        }

        public int Position { get; set; }
        public string Hash { get; set; }
        public string SourceAddress { get; set; }
        public long Amount { get; set; }

        public override string ToString()
        {
            return $"{Position};{Hash};{SourceAddress};{Amount}";
        }
    }
}
