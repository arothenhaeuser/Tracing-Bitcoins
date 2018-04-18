using MongoDB.Bson.Serialization.Attributes;
using NBitcoin;

namespace fd.Coins.Core
{
    public class MyTestClass : Transaction
    {
        [BsonId]
        public uint256 Hash
        {
            get
            {
                return GetHash();
            }
        }

        public MyTestClass(Transaction tx)
            : base(tx.ToBytes())
        {
        }
    }
}
