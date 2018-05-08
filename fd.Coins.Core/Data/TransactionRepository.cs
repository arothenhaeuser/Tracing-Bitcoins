using Dapper;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Core.NetworkConnector
{
    public class TransactionRepository : Repository<TransactionEntity>
    {
        public TransactionRepository(string connectionString, string table) : base(connectionString, table) { }

        public override bool Add(TransactionEntity item)
        {
            using (var conn = Connection)
            {
                try
                {
                    var sql = $"INSERT INTO {_table} (hash, blockTime) VALUES (@hash, @blockTime);";
                    var sql2 = $"INSERT INTO {_table}_inputs (position, hash, sourceAddress, amount) VALUES (@position, @hash, @sourceAddress, @amount);";
                    var sql3 = $"INSERT INTO {_table}_outputs (position, hash, targetAddress, amount) VALUES (@position, @hash, @targetAddress, @amount);";
                    using (var trans = conn.BeginTransaction())
                    {
                        conn.Execute(sql, new { item.Hash, item.BlockTime });
                        conn.Execute(sql2, item.Inputs.Select(x => new { x.Position, x.Hash, x.SourceAddress, x.Amount }));
                        conn.Execute(sql3, item.Outputs.Select(x => new { x.Position, x.Hash, x.TargetAddress, x.Amount }));
                        trans.Commit();
                    }
                }
                catch (MySqlException e)
                {
                    if (e.Number == 1062)
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
                    var sql = $"INSERT INTO {_table} (hash, blockTime) VALUES (@hash, @blockTime);";
                    var sql2 = $"INSERT INTO {_table}_inputs (position, hash, sourceAddress, amount) VALUES (@position, @hash, @sourceAddress, @amount);";
                    var sql3 = $"INSERT INTO {_table}_outputs (position, hash, targetAddress, amount) VALUES (@position, @hash, @targetAddress, @amount);";

                    conn.Open();

                    using (var trans = conn.BeginTransaction())
                    {
                        conn.Execute(sql, items.Select(x => new { x.Hash, x.BlockTime }));
                        conn.Execute(sql2, items.SelectMany(x => x.Inputs.Select(y => new { y.Position, y.Hash, y.SourceAddress, y.Amount })));
                        conn.Execute(sql3, items.SelectMany(x => x.Outputs.Select(y => new { y.Position, y.Hash, y.TargetAddress, y.Amount })));
                        trans.Commit();
                    }
                }
                catch (Exception e)
                {
                    if(e is MySqlException)
                    {
                        if ((e as MySqlException).Number == 1062)
                        {
                            return true;
                        }
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
                    var sql = $"CREATE TABLE IF NOT EXISTS {_table} (hash varchar(64) PRIMARY KEY, blockTime timestamp);"
                        + $"CREATE TABLE IF NOT EXISTS {_table}_inputs (position int, hash varchar(64), sourceAddress varchar(255), amount bigint, PRIMARY KEY (position, hash), FOREIGN KEY (hash) REFERENCES {_table}(hash) ON DELETE CASCADE);"
                        + $"CREATE TABLE IF NOT EXISTS {_table}_outputs (position int, hash varchar(64), targetAddress varchar(255), amount bigint, PRIMARY KEY (position, hash), FOREIGN KEY (hash) REFERENCES {_table}(hash) ON DELETE CASCADE);"
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
                    //var sql = $"SELECT * FROM {_table};SELECT * FROM {_table}_inputs;SELECT * FROM {_table}_outputs;";
                    var sql2 = $"SELECT tx.hash, tx.blockTime, input.position, input.hash, input.sourceAddress, input.amount, output.position, output.hash, output.targetAddress, output.amount FROM {_table} tx JOIN {_table}_inputs input ON tx.hash = input.hash JOIN {_table}_outputs output ON tx.hash = output.hash;";
                    var lookup = new Dictionary<string, TransactionEntity>();
                    return conn.Query<TransactionEntity, TxInput, TxOutput, TransactionEntity>(sql2, (tx, input, output) =>
                    {
                        TransactionEntity t;
                        if (!lookup.TryGetValue(tx.Hash, out t))
                        {
                            lookup.Add(tx.Hash, t = tx);
                        }
                        if (t.Inputs == null)
                        {
                            t.Inputs = new List<TxInput>();
                        }
                        t.Inputs.Add(input);
                        if (t.Outputs == null)
                        {
                            t.Outputs = new List<TxOutput>();
                        }
                        tx.Outputs.Add(output);
                        return t;
                    }, splitOn:"position,position", buffered: false);
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
                    var sql = $"SELECT tx.hash, tx.blockTime, input.position, input.hash, input.sourceAddress, input.amount, output.position, output.hash, output.targetAddress, output.amount FROM {_table} tx JOIN {_table}_inputs input ON tx.hash = input.hash JOIN {_table}_outputs output ON tx.hash = output.hash WHERE tx.hash=@hash;";
                    var lookup = new Dictionary<string, TransactionEntity>();
                    return conn.Query<TransactionEntity, TxInput, TxOutput, TransactionEntity>(sql, (tx, input, output) =>
                    {
                        TransactionEntity t;
                        if(!lookup.TryGetValue(tx.Hash, out t))
                        {
                            lookup.Add(tx.Hash, t = tx);
                        }
                        if(t.Inputs == null)
                        {
                            t.Inputs = new List<TxInput>();
                        }
                        t.Inputs.Add(input);
                        if (t.Outputs == null)
                        {
                            t.Outputs = new List<TxOutput>();
                        }
                        tx.Outputs.Add(output);
                        return t;
                    },
                    new { hash },
                    splitOn: "position,position", buffered: false).FirstOrDefault();
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

                    conn.Execute(sql, items.Select(x => new { x.Hash, x.BlockTime }), transaction: trans, commandTimeout: 30);
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
