using Orient.Client;
using System;

namespace fd.Coins.AFistfulOfBitcoins
{
    public static class Utils
    {
        public static void ResetPreviousRun()
        {
            // check if an old version of the resulting graph already exists and delete it
            var server = new OServer("localhost", 2424, "root", "root");
            if (server.DatabaseExist("addressclusters", OStorageType.PLocal))
            {
                server.DropDatabase("addressclusters", OStorageType.PLocal);
            }
            // create a new graph for the address clusters
            server.CreateDatabase("addressclusters", ODatabaseType.Graph, OStorageType.PLocal);
            using (var db = new ODatabase("localhost", 2424, "addressclusters", ODatabaseType.Graph, "admin", "admin"))
            {
                db.Command("CREATE CLASS Node IF NOT EXISTS EXTENDS V");
                db.Command("CREATE PROPERTY Node.Address STRING");
                db.Command("CREATE CLASS Fistful IF NOT EXISTS EXTENDS E");
                db.Command("CREATE PROPERTY Fistful.Tag STRING");
                db.Command("CREATE INDEX IndexForAddress ON Node (Address) UNIQUE_HASH_INDEX");
            }
        }

        public static void RetryOnConcurrentFail(int attempts, Func<bool> p)
        {
            var count = attempts;
            while (count > 0)
            {
                try
                {
                    if (p.Invoke())
                        return;
                }
                catch (Exception e)
                {
                    count--;
                }
            }
        }
    }
}
