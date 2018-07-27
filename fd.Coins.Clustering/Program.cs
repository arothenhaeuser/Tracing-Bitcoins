using fd.Coins.Core.Clustering.Intrinsic;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fd.Coins.Clustering
{
    class Program
    {
        static void Main(string[] args)
        {
            //var timeSlots = new TimeSlots();
            //timeSlots.Run(new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" });
            //timeSlots.ToFile("report");
            var totalAmounts = new TotalAmounts();
            totalAmounts.Run(new ConnectionOptions() { DatabaseName = "txgraph", DatabaseType = ODatabaseType.Graph, HostName = "localhost", Password = "admin", Port = 2424, UserName = "admin" });
            totalAmounts.ToFile("report");
        }
    }
}
