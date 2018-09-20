using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Clustering
{
    public class DataSourceProvider
    {
        private DataSourceOptions _options;

        public DataSourceProvider()
        {
            _options = new DataSourceOptions("", LimitType.DEPTH, 8);
        }

        public DataSourceProvider(string startTx, LimitType type, int limit)
        {
            _options = new DataSourceOptions(startTx, type, limit);
        }

        public List<ORID> GetRids(ConnectionOptions dbOptions)
        {
            List<ORID> result = null;
            using(var db = new ODatabase(dbOptions))
            {
                switch (_options.Type)
                {
                    case LimitType.DEPTH:
                        result = db.Query<ODocument>($"TRAVERSE out() FROM (SELECT * FROM Transaction WHERE Hash = '{_options.StartTx}') WHILE $depth <= {_options.Limit} STRATEGY BREADTH_FIRST").Select(x => x.GetField<ORID>("@ORID")).ToList();
                        break;
                    case LimitType.DATE:
                        var start = db.Query<ODocument>($"SELECT BlockTime AS value FROM Transaction WHERE Hash = '{_options.StartTx}'").Select(x => x.GetField<DateTime>("value")).FirstOrDefault();
                        var end = start.AddMinutes(_options.Limit);
                        result = db.Query<ODocument>($"SELECT * FROM Transaction WHERE BlockTime > '{start.ToString("yyyy-MM-dd hh:mm:ss")}' AND BlockTime < '{end.ToString("yyyy-MM-dd hh:mm:ss")}'").ToList().Select(x => x.GetField<ORID>("@ORID")).ToList();
                        break;
                    case LimitType.COUNT:
                        result = db.Query<ODocument>($"TRAVERSE out() FROM (SELECT * FROM Transaction WHERE Hash = '{_options.StartTx}') LIMIT {_options.Limit} STRATEGY BREADTH_FIRST").Select(x => x.GetField<ORID>("@ORID")).ToList();
                        break;
                    default:
                        result = new List<ORID>();
                        break;
                }
                return result;
            }
        }

        public List<string> GetAddresses(ConnectionOptions dbOptions)
        {
            List<string> result = null;
            using (var db = new ODatabase(dbOptions))
            {
                switch (_options.Type)
                {
                    case LimitType.DEPTH:
                        result = db.Command($"SELECT list(inE().tAddr) as addresses FROM (TRAVERSE out() FROM (SELECT * FROM Transaction WHERE Hash = '{_options.StartTx}') WHILE $depth <= {_options.Limit} STRATEGY BREADTH_FIRST)").ToList().FirstOrDefault().GetField<List<string>>("addresses").Where(x => !string.IsNullOrEmpty(x)).Distinct().ToList();
                        break;
                    case LimitType.DATE:
                        var start = db.Query<ODocument>($"SELECT BlockTime AS value FROM Transaction WHERE Hash = '{_options.StartTx}'").Select(x => x.GetField<DateTime>("value")).FirstOrDefault();
                        var end = start.AddMinutes(_options.Limit);
                        result = db.Command($"SELECT list(inE().tAddr) as addresses FROM Transaction WHERE BlockTime > '{start.ToString("yyyy-MM-dd hh:mm:ss")}' AND BlockTime < '{end.ToString("yyyy-MM-dd hh:mm:ss")}'").ToList().FirstOrDefault().GetField<List<string>>("addresses").Where(x => !string.IsNullOrEmpty(x)).Distinct().ToList();
                        break;
                    case LimitType.COUNT:
                        result = db.Command($"SELECT list(inE().tAddr) as addresses FROM (TRAVERSE out() FROM (SELECT * FROM Transaction WHERE Hash = '{_options.StartTx}') LIMIT {_options.Limit} STRATEGY BREADTH_FIRST)").ToList().FirstOrDefault().GetField<List<string>>("addresses").Where(x => !string.IsNullOrEmpty(x)).Distinct().ToList();
                        break;
                    default:
                        result = new List<string>();
                        break;
                }
                return result;
            }
        }

        private long DaysToMs(int days)
        {
            return days * 24 * 60 * 60 * 1000;
        }
    }

    public class DataSourceOptions
    {
        public DataSourceOptions(string startTx, LimitType type, int limit)
        {
            StartTx = startTx;
            Type = type;
            Limit = limit;
        }
        public string StartTx { get; private set; }
        public LimitType Type { get; private set; }
        public int Limit { get; private set; }
    }

    public enum LimitType
    {
        DEPTH = 0,
        DATE = 1,
        COUNT = 2
    }
}
