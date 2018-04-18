using System;

namespace fd.Coins.Core
{
    public class TxOutput
    {
        public TxOutput() { }
        public TxOutput(string s)
        {
            try
            {
                var components = s.Split(';');
                Hash = components[0];
                TargetAddress = components[1];
                Amount = long.Parse(components[2]);
            }
            catch
            {
                throw new ArgumentException("The string supplied is invalid.");
            }
        }
        public TxOutput(string hash, string targetAddress, long amount)
        {
            Hash = hash;
            TargetAddress = targetAddress;
            Amount = amount;
        }

        public string Hash { get; set; }
        public string TargetAddress { get; set; }
        public long Amount { get; set; }

        public override string ToString()
        {
            return $"{Hash};{TargetAddress};{Amount}";
        }
    }
}
