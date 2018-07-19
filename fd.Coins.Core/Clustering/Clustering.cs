using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fd.Coins.Core.Clustering
{
    interface Clustering
    {
        void Run(ConnectionOptions mainOptions);
        void ToFile(string path);
    }
}
