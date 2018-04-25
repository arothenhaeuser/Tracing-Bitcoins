using System;

namespace fd.Coins.Core.NetworkConnector
{
    public class TxOutput
    {
        public TxOutput() { }
        public TxOutput(string s)
        {
            try
            {
                var components = s.Split(';');
                Position = int.Parse(components[0]);
                Hash = components[1];
                TargetAddress = components[2];
                Amount = long.Parse(components[3]);
            }
            catch
            {
                throw new ArgumentException("The string supplied is invalid.");
            }
        }
        public TxOutput(int position, string hash, string targetAddress, long amount)
        {
            Position = position;
            Hash = hash;
            TargetAddress = targetAddress;
            Amount = amount;
        }

        public int Position { get; set; }
        public string Hash { get; set; }
        public string TargetAddress { get; set; }
        public long Amount { get; set; }

        public override string ToString()
        {
            return $"{Position};{Hash};{TargetAddress};{Amount}";
        }
    }
}
