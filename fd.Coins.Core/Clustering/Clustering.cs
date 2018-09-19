using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System.Collections.Generic;

namespace fd.Coins.Core.Clustering
{
    public abstract class Clustering
    {
        protected ConnectionOptions _options;

        protected int _dimension;

        public abstract void Run(ConnectionOptions mainOptions, IEnumerable<string> rids);
        public abstract double Distance(string addr1, string addr2);
        public abstract void ToFile(string path);
        public abstract void FromFile(string path);

        public void Recreate()
        {
            // check if an old version of the resulting graph already exists and delete it
            var server = new OServer("localhost", 2424, "root", "root");
            if (server.DatabaseExist(_options.DatabaseName, OStorageType.PLocal))
            {
                server.DropDatabase(_options.DatabaseName, OStorageType.PLocal);
            }
            // create a new graph for the address clusters
            server.CreateDatabase(_options.DatabaseName, _options.DatabaseType, OStorageType.PLocal);
            using (var db = new ODatabase(_options))
            {
                db.Command("CREATE CLASS Node EXTENDS V");
                db.Command("CREATE PROPERTY Node.Address STRING");
                db.Command($"CREATE CLASS {_options.DatabaseName} EXTENDS E");
                db.Command("CREATE INDEX IndexForAddress ON Node (Address) UNIQUE_HASH_INDEX");
            }
        }
    }
}
