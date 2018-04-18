﻿using System;

namespace fd.Coins.Core.NetworkConnector
{
    public class TxInput
    {
        public TxInput() { }
        public TxInput(string hash, string prevOut)
        {
            try
            {
                var components = prevOut.Split(';');
                Hash = hash;
                SourceAddress = components[1];
                Amount = long.Parse(components[2]);
            }
            catch
            {
                throw new ArgumentException("The string supplied is invalid.");
            }
        }
        public TxInput(string hash, string sourceAddress, long amount)
        {
            Hash = hash;
            SourceAddress = sourceAddress;
            Amount = amount;
        }

        public string Hash { get; set; }
        public string SourceAddress { get; set; }
        public long Amount { get; set; }

        public override string ToString()
        {
            return $"{Hash};{SourceAddress};{Amount}";
        }
    }
}