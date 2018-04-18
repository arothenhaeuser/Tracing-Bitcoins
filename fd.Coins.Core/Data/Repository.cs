﻿using Dapper;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace fd.Coins.Core.NetworkConnector
{
    public abstract class Repository<T> : IRepository<T>
    {
        protected readonly string _connectionString;
        protected readonly string _table;

        public int Count
        {
            get
            {
                using (var conn = Connection)
                {
                    try
                    {
                        var sql = $"SELECT count(*) from {_table};";
                        return conn.ExecuteScalar<int>(sql);
                    }
                    catch (Exception e)
                    {
                        return -1;
                    }
                }
            }
        }

        protected IDbConnection Connection
        {
            get
            {
                return new MySqlConnection(_connectionString);
            }
        }

        public Repository(string connectionString, string table)
        {
            _connectionString = connectionString;
            _table = table;
            CreateTablesIfNotExist();
        }
        protected abstract void CreateTablesIfNotExist();

        public abstract bool Add(T item);
        public abstract bool AddRange(IEnumerable<T> items);
        public abstract IEnumerable<T> GetAll();
        public abstract T GetById(string id);
        public abstract bool Remove(T item);
        public abstract bool Remove(IEnumerable<T> items);
        public abstract bool Update(T item);
        public abstract bool Update(IEnumerable<T> items);
    }
}