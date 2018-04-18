using Dapper;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Core.NetworkConnector
{
    public class TransactionRepository : Repository<TransactionEntity>
    {
        // these two exist in the blockchain two times
        public static string[] KNOWN_DUPLICATE_TRANSACTION_HASHES
        {
            get
            {
                return new string[]
                {
                    "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599",
                    "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468"
                };
            }
        }

        public TransactionRepository(string connectionString, string table) : base(connectionString, table) { }

        public override bool Add(TransactionEntity item)
        {
            using (var conn = Connection)
            {
                try
                {
                    var sql = $"INSERT INTO {_table} (hash, blockTime, payload) VALUES (@hash, @blockTime, @payload);";
                    var sql2 = $"INSERT INTO {_table}_inputs (hash, sourceAddress, amount) VALUES (@hash, @sourceAddress, @amount);";
                    var sql3 = $"INSERT INTO {_table}_outputs (hash, targetAddress, amount) VALUES (@hash, @targetAddress, @amount);";
                    using (var trans = conn.BeginTransaction())
                    {
                        conn.Execute(sql, new { item.Hash, item.BlockTime, item.Payload });
                        conn.Execute(sql2, item.Inputs.Select(x => new { x.Hash, x.SourceAddress, x.Amount }));
                        conn.Execute(sql3, item.Outputs.Select(x => new { x.Hash, x.TargetAddress, x.Amount }));
                        trans.Commit();
                    }
                }
                catch (MySqlException e)
                {
                    if (e.Number == 1062 && KNOWN_DUPLICATE_TRANSACTION_HASHES.Any(x => e.Message.Contains(x)))
                    {
                        return true;
                    }
                    return false;
                }
                return true;
            }
        }

        public override bool AddRange(IEnumerable<TransactionEntity> items)
        {
            using (var conn = Connection)
            {
                try
                {
                    var sql = $"INSERT INTO {_table} (hash, blockTime, payload) VALUES (@hash, @blockTime, @payload);";
                    var sql2 = $"INSERT INTO {_table}_inputs (hash, sourceAddress, amount) VALUES (@hash, @sourceAddress, @amount);";
                    var sql3 = $"INSERT INTO {_table}_outputs (hash, targetAddress, amount) VALUES (@hash, @targetAddress, @amount);";

                    conn.Open();

                    using (var trans = conn.BeginTransaction())
                    {
                        conn.Execute(sql, items.Select(x => new { x.Hash, x.BlockTime, x.Payload }));
                        conn.Execute(sql2, items.SelectMany(x => x.Inputs.Select(y => new { y.Hash, y.SourceAddress, y.Amount })));
                        conn.Execute(sql2, items.SelectMany(x => x.Outputs.Select(y => new { y.Hash, y.TargetAddress, y.Amount })));
                        trans.Commit();
                    }
                }
                catch (MySqlException e)
                {
                    if (e.Number == 1062 && KNOWN_DUPLICATE_TRANSACTION_HASHES.Any(x => e.Message.Contains(x)))
                    {
                        return true;
                    }
                    return false;
                }
                return true;
            }
        }

        protected override void CreateTablesIfNotExist()
        {
            using (var conn = Connection)
            {
                try
                {
                    var sql = $"CREATE TABLE IF NOT EXISTS {_table} (hash varchar(64) PRIMARY KEY, blockTime timestamp, payload blob);"
                        + $"CREATE TABLE IF NOT EXISTS {_table}_inputs (id int NOT NULL AUTO_INCREMENT PRIMARY KEY, hash varchar(64), sourceAddress varchar(255), amount bigint, FOREIGN KEY (hash) REFERENCES {_table}(hash));"
                        + $"CREATE TABLE IF NOT EXISTS {_table}_outputs (id int NOT NULL AUTO_INCREMENT PRIMARY KEY, hash varchar(64), targetAddress varchar(255), amount bigint, FOREIGN KEY (hash) REFERENCES {_table}(hash));"
                        + "SET time_zone='+00:00';";
                    conn.Execute(sql);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Table '{_table}' could not be created.");
                }
            }
        }

        public override IEnumerable<TransactionEntity> GetAll()
        {
            using (var conn = Connection)
            {
                try
                {
                    var sql = $"SELECT * FROM {_table};SELECT * FROM {_table}_inputs;SELECT * FROM {_table}_outputs;";
                    var result = conn.QueryMultiple(sql);
                    var txEntities = result.Read<TransactionEntity>();
                    foreach (var txEntity in txEntities)
                    {
                        txEntity.Inputs.AddRange(result.Read<TxInput>().Where(x => x.Hash == txEntity.Hash));
                        txEntity.Outputs.AddRange(result.Read<TxOutput>().Where(x => x.Hash == txEntity.Hash));
                    }
                    return txEntities;
                }
                catch (Exception e)
                {
                    return default(IEnumerable<TransactionEntity>);
                }
            }
        }

        public override TransactionEntity GetById(string hash)
        {
            using (var conn = Connection)
            {
                try
                {
                    var sql = $"SELECT * FROM {_table} WHERE hash = @hash;SELECT * FROM {_table}_inputs WHERE hash = @hash;SELECT * FROM {_table}_outputs WHERE hash = @hash;";
                    var result = conn.QueryMultiple(sql, new { hash });
                    var txEntity = result.Read<TransactionEntity>().SingleOrDefault();
                    if (txEntity != default(TransactionEntity))
                    {
                        txEntity.Inputs = result.Read<TxInput>().ToList();
                        txEntity.Outputs = result.Read<TxOutput>().ToList();
                    }
                    return txEntity;
                }
                catch (Exception e)
                {
                    return default(TransactionEntity);
                }
            }
        }

        public override bool Remove(TransactionEntity item)
        {
            throw new NotImplementedException();
        }

        public override bool Remove(IEnumerable<TransactionEntity> items)
        {
            throw new NotImplementedException();
        }

        public override bool Update(TransactionEntity item)
        {
            throw new NotImplementedException();
        }

        public override bool Update(IEnumerable<TransactionEntity> items)
        {
            using (var conn = Connection)
            {
                try
                {
                    var sql = $"UPDATE {_table} SET blockTime=@blocktime, payload=@payload) WHERE hash=@hash;";
                    var sql1 = $"DELETE FROM {_table}_inputs WHERE hash=@hash;DELETE FROM {_table}_outputs WHERE hash=@hash;";
                    var sql2 = $"INSERT INTO {_table}_inputs (hash, sourceAddress, amount) VALUES (@hash, @sourceAddress, @amount);";
                    var sql3 = $"INSERT INTO {_table}_outputs (hash, targetAddress, amount) VALUES (@hash, @targetAddress, @amount);";

                    conn.Open();
                    var trans = conn.BeginTransaction();

                    conn.Execute(sql, items.Select(x => new { x.Hash, x.BlockTime, x.Payload }), transaction: trans, commandTimeout: 30);
                    conn.Execute(sql1, items.Select(x => new { x.Hash }), transaction: trans, commandTimeout: 30);

                    foreach (var item in items)
                    {
                        conn.Execute(sql2, item.Inputs.Select(x => new { x.Hash, x.SourceAddress, x.Amount }), transaction: trans, commandTimeout: 30);
                        conn.Execute(sql3, item.Outputs.Select(x => new { x.Hash, x.TargetAddress, x.Amount }), transaction: trans, commandTimeout: 30);
                    }

                    trans.Commit();
                    conn.Close();
                }
                catch (Exception e)
                {
                    return false;
                }
                finally
                {
                    conn.Close();
                }
                return true;
            }
        }
    }
}
