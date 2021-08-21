using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;

namespace metastrings
{
    /// <summary>
    /// Context manages the database connection
    /// and provides useful query helper functions
    /// </summary>
    public class Context : IDisposable
    {
        /// <summary>
        /// Create a context for a database connection
        /// </summary>
        /// <param name="dbConnStr">Database connection string...we're out of the config business</param>
        public Context(string dbConnStr)
        {
            string actualDbConnStr;
            if (!sm_dbConnStrs.TryGetValue(dbConnStr, out actualDbConnStr))
            {
                lock (sm_dbBuildLock)
                {
                    if (!sm_dbConnStrs.TryGetValue(dbConnStr, out actualDbConnStr))
                    {
                        actualDbConnStr = dbConnStr;

                        if (!IsDbServer(actualDbConnStr))
                        {
                            string dbFilePath = DbConnStrToFilePath(actualDbConnStr);
                            actualDbConnStr = "Data Source=" + dbFilePath;

                            if (!(File.Exists(dbFilePath) && new FileInfo(dbFilePath).Length > 0))
                            {
                                SQLiteConnection.CreateFile(dbFilePath);

                                using (var db = new SqlLiteDb(actualDbConnStr))
                                {
                                    RunSql(db, Tables.CreateSql);
                                    RunSql(db, Names.CreateSql);
                                    RunSql(db, Values.CreateSql);
                                    RunSql(db, Items.CreateSql);
                                    RunSql(db, LongStrings.CreateSql);
                                }
                            }

                            using (var db = new SqlLiteDb(actualDbConnStr))
                                RunSql(db, new[] { "PRAGMA journal_mode = WAL", "PRAGMA synchronous = NORMAL" });
                        }

                        sm_dbConnStrs[dbConnStr] = actualDbConnStr;
                    }
                }
            }

            IsServerDb = IsDbServer(actualDbConnStr);

            if (IsServerDb)
                Db = new MySqlDb(actualDbConnStr);
            else
                Db = new SqlLiteDb(actualDbConnStr);
        }

        public void Dispose()
        {
            if (Db != null)
            {
                Db.Dispose();
                Db = null;
            }

            if (m_postItemOps != null && m_postItemOps.Count > 0)
                throw new MetaStringsException("Post ops remain; call ProcessPostOpsAsync before disposing the metastrings context");
        }

        /// <summary>
        /// The database connection
        /// </summary>
        public IDb Db { get; private set; }

        /// <summary>
        /// See if it's MySQL, not SQLite
        /// </summary>
        public bool IsServerDb { get; private set; }

        /// <summary>
        /// Create a new Command object using this Context
        /// </summary>
        public Command Cmd => new Command(this);

        /// <summary>
        /// Transactions are supported, 
        /// but should not be used around any code affecting data 
        /// in the Table, Name, Value, etc. metastrings database
        /// as rollbacks would break the global in-memory caching
        /// </summary>
        /// <returns>Transaction object</returns>
        public MsTrans BeginTrans()
        {
            return Db.BeginTrans();
        }

        /// <summary>
        /// Query helper function to get a reader for a query
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>Reader to get results from</returns>
        public async Task<DbDataReader> ExecSelectAsync(Select select)
        {
            var cmdParams = select.cmdParams;
            var sql = await Sql.GenerateSqlAsync(this, select).ConfigureAwait(false);
            return await Db.ExecuteReaderAsync(sql, cmdParams).ConfigureAwait(false);
        }

        /// <summary>
        /// Query helper function to get a single value for a query
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>The single query result value</returns>
        public async Task<object> ExecScalarAsync(Select select)
        {
            var cmdParams = select.cmdParams;
            var sql = await Sql.GenerateSqlAsync(this, select).ConfigureAwait(false);
            return await Db.ExecuteScalarAsync(sql, cmdParams).ConfigureAwait(false);
        }

        /// <summary>
        /// Query helper to get a single 64-bit integer query result
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>64-bit result value, or -1 if processing fails</returns>
        public async Task<long> ExecScalar64Async(Select select)
        {
            object result = await ExecScalarAsync(select).ConfigureAwait(false);
            long val = Utils.ConvertDbInt64(result);
            return val;
        }

        /// <summary>
        /// Query helper to get a list of results from a single-column query
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>List of results of type T</returns>
        public async Task<List<T>> ExecListAsync<T>(Select select)
        {
            var values = new List<T>();
            using (var reader = await ExecSelectAsync(select).ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                    values.Add((T)reader.GetValue(0));
            }
            return values;
        }

        /// <summary>
        /// Query helper to get a dictionary of results from a single-column query
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>ListDictionary of results of type K, V</returns>
        public async Task<ListDictionary<K, V>> ExecDictAsync<K, V>(Select select)
        {
            var values = new ListDictionary<K, V>();
            using (var reader = await ExecSelectAsync(select).ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                    values.Add((K)reader.GetValue(0), (V)reader.GetValue(1));
            }
            return values;
        }

        /// <summary>
        /// Get the items table row ID for a given table and key
        /// </summary>
        /// <param name="tableName">Table to look in</param>
        /// <param name="key">Key of the item in the table</param>
        /// <returns>Row ID, or -1 if not found</returns>
        public async Task<long> GetRowIdAsync(string tableName, object key)
        {
            Utils.ValidateTableName(tableName, "GetRowId");
            Select select = Sql.Parse($"SELECT id FROM {tableName} WHERE value = @value");
            select.AddParam("@value", key);
            long id = await ExecScalar64Async(select).ConfigureAwait(false);
            return id;
        }

        /// <summary>
        /// Get the object value from the given table and items table ID
        /// </summary>
        /// <param name="table">Table to look in</param>
        /// <param name="id">Row ID to look for</param>
        /// <returns>object value if found, null otherwise</returns>
        public async Task<object> GetRowValueAsync(string table, long id)
        {
            Utils.ValidateTableName(table, "GetRowValueAsync");
            Select select = Sql.Parse($"SELECT value FROM {table} WHERE id = @id");
            select.AddParam("@id", id);
            object val = await ExecScalarAsync(select).ConfigureAwait(false);
            return val;
        }

        /// <summary>
        /// Process queries that piled up by Command's Define function
        /// This is the rare case where using a transaction is well-advised
        /// </summary>
        public async Task ProcessPostOpsAsync()
        {
            if (m_postItemOps == null || m_postItemOps.Count == 0)
                return;

            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                using (var msTrans = BeginTrans())
                {
                    foreach (string sql in m_postItemOps)
                        await Db.ExecuteSqlAsync(sql).ConfigureAwait(false);
                    msTrans.Commit();
                }
            }
            finally
            {
                m_postItemOps.Clear();
                ScopeTiming.RecordScope("ProcessItemPostOps", totalTimer);
            }
        }

        internal void AddPostOp(string sql)
        {
            if (m_postItemOps == null)
                m_postItemOps = new List<string>();
            m_postItemOps.Add(sql);
        }

        internal void ClearPostOps()
        {
            if (m_postItemOps != null)
                m_postItemOps.Clear();
        }
        private List<string> m_postItemOps;

        private static string DbConnStrToFilePath(string connStr)
        {
            if (IsDbServer(connStr))
                throw new MetaStringsException("Connection string is not for file-based DB");

            string filePath = connStr;

            int equals = filePath.IndexOf('=');
            if (equals > 0)
                filePath = filePath.Substring(equals + 1);

            filePath = filePath.Replace("[UserRoaming]", Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData));
            filePath = filePath.Replace("[MyDocuments]", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments));
    
            string directoryPath = Path.GetDirectoryName(filePath);
            if (!Directory.Exists(directoryPath))
                Directory.CreateDirectory(directoryPath);
            
            return filePath;
        }

        private static bool IsDbServer(string connStr)
        {
            bool isServer = connStr.IndexOf("Server=", 0, StringComparison.OrdinalIgnoreCase) >= 0;
            return isServer;
        }

        private static void RunSql(IDb db, string[] sqlQueries)
        {
            foreach (string sql in sqlQueries)
                db.ExecuteSql(sql);
        }

        private static object sm_dbBuildLock = new object();
        private static ConcurrentDictionary<string, string> sm_dbConnStrs = new ConcurrentDictionary<string, string>();
    }
}
