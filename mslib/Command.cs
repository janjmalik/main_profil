using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace metastrings
{
    /// <summary>
    /// Command implements the metastrings API.
    /// Each function takes an input parameters class and returns a response parameters class.
    /// This stemmed from earlier code which supported a JSON-in / JSON-out standalone application server.
    /// </summary>
    public class Command
    {
        /// <summary>
        /// A Command needs a Context for accessing the database.
        /// </summary>
        /// <param name="ctxt">Object used for accessing the database</param>
        public Command(Context ctxt)
        {
            Ctxt = ctxt;
        }

        /// <summary>
        /// This is the main UPSERT method to populate the database.
        /// </summary>
        /// <param name="define">Info about metadata to apply to the key</param>
        public async Task DefineAsync(Define define)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                var localTimer = ScopeTiming.StartTiming();

                bool isKeyNumeric = !(define.key is string);
                int tableId = await Tables.GetIdAsync(Ctxt, define.table, isKeyNumeric).ConfigureAwait(false);
                long valueId = await Values.GetIdAsync(Ctxt, define.key).ConfigureAwait(false);
                long itemId = await Items.GetIdAsync(Ctxt, tableId, valueId).ConfigureAwait(false);
                ScopeTiming.RecordScope("Define.Setup", localTimer);

                if (define.metadata != null)
                {
                    // name => nameid
                    var nameValueIds = new Dictionary<int, long>();
                    foreach (var kvp in define.metadata)
                    {
                        bool isMetadataNumeric = !(kvp.Value is string);
                        int nameId = await Names.GetIdAsync(Ctxt, tableId, kvp.Key, isMetadataNumeric).ConfigureAwait(false);
                        if (kvp.Value == null) // erase value
                        {
                            nameValueIds[nameId] = -1;
                            continue;
                        }
                        bool isNameNumeric = await Names.GetNameIsNumericAsync(Ctxt, nameId).ConfigureAwait(false);
                        bool isValueNumeric = !(kvp.Value is string);
                        if (isValueNumeric != isNameNumeric)
                        {
                            throw
                                new MetaStringsException
                                (
                                    $"Data numeric does not match name: {kvp.Key}" +
                                    $"\n - value is numeric: {isValueNumeric} - {kvp.Value}" +
                                    $"\n - name is numeric: {isNameNumeric}"
                                );
                        }
                        nameValueIds[nameId] =
                            await Values.GetIdAsync(Ctxt, kvp.Value).ConfigureAwait(false);
                    }
                    ScopeTiming.RecordScope("Define.NameIds", localTimer);

                    Items.SetItemData(Ctxt, itemId, nameValueIds);
                    ScopeTiming.RecordScope("Define.ItemsCommit", localTimer);
                }

                await Ctxt.ProcessPostOpsAsync().ConfigureAwait(false);
                ScopeTiming.RecordScope("Define.PostOps", localTimer);
            }
#if !DEBUG
            catch
            {
                Ctxt.ClearPostOps();
                throw;
            }
#endif
            finally
            {
                ScopeTiming.RecordScope("Define", totalTimer);
            }
        }

        /// <summary>
        /// Generate SQL query given a Select object
        /// This is where the metastrings -> SQL magic happens
        /// </summary>
        /// <param name="query">NoSQL query object</param>
        /// <returns>SQL query</returns>
        public async Task<string> GenerateSqlAsync(Select query)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                string sql = await Sql.GenerateSqlAsync(Ctxt, query).ConfigureAwait(false);
                return sql;
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.GenerateSql", totalTimer);
            }
        }

        /// <summary>
        /// Get the metadata for a set of items
        /// </summary>
        /// <param name="request">List of values to get metadata for</param>
        /// <returns>Metadata for the items</returns>
        public async Task<GetResponse> GetAsync(GetRequest request)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                var responses = new List<Dictionary<string, object>>(request.values.Count);

                int tableId = await Tables.GetIdAsync(Ctxt, request.table, noCreate: true).ConfigureAwait(false);
                foreach (var value in request.values)
                {
                    long valueId = await Values.GetIdAsync(Ctxt, value).ConfigureAwait(false);

                    long itemId = await Items.GetIdAsync(Ctxt, tableId, valueId, noCreate: true).ConfigureAwait(false);
                    if (itemId < 0)
                    {
                        responses.Add(null);
                        continue;
                    }

                    var metaIds = await Items.GetItemDataAsync(Ctxt, itemId).ConfigureAwait(false);
                    var metaStrings = await NameValues.GetMetadataValuesAsync(Ctxt, metaIds).ConfigureAwait(false);

                    responses.Add(metaStrings);
                }

                GetResponse response = new GetResponse() { metadata = responses };
                return response;
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.Get", totalTimer);
            }
        }

        /// <summary>
        /// Query for the metadata for a set of items.
        /// </summary>
        /// <param name="request">NoSQL query for items to get</param>
        /// <returns>Metadata of found items</returns>
        public async Task<GetResponse> QueryGetAsync(QueryGetRequest request)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                var itemValues = new Dictionary<long, object>();
                {
                    Select select = new Select();
                    select.select = new List<string> { "id", "value" };
                    select.from = request.from;
                    select.where = request.where;
                    select.orderBy = request.orderBy;
                    select.limit = request.limit;
                    select.cmdParams = request.cmdParams;
                    using (var reader = await Ctxt.ExecSelectAsync(select).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                            itemValues.Add(reader.GetInt64(0), reader.GetValue(1));
                    }
                }

                var responses = new List<Dictionary<string, object>>(itemValues.Count);
                foreach (var itemId in itemValues.Keys)
                {
                    var metaIds = await Items.GetItemDataAsync(Ctxt, itemId).ConfigureAwait(false);
                    var metaStrings = await NameValues.GetMetadataValuesAsync(Ctxt, metaIds).ConfigureAwait(false);

                    metaStrings["id"] = (double)itemId;
                    metaStrings["value"] = itemValues[itemId];

                    responses.Add(metaStrings);
                }

                GetResponse response = new GetResponse() { metadata = responses };
                return response;
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.QueryGet", totalTimer);
            }
        }

        /// <summary>
        /// Delete a single item from a table.
        /// </summary>
        /// <param name="table">Table to delete from</param>
        /// <param name="value">Value of object to delete</param>
        public async Task DeleteAsync(string table, object value)
        {
            await DeleteAsync(new Delete(table, value)).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete multiple items from a table.
        /// </summary>
        /// <param name="table">Table to delete from</param>
        /// <param name="values">Values of objects to delete</param>
        public async Task DeleteAsync(string table, IEnumerable<object> values)
        {
            await DeleteAsync(new Delete(table, values)).ConfigureAwait(false);
        }

        /// <summary>
        /// Process a delete request.
        /// </summary>
        /// <param name="toDelete">Delete reqeuest</param>
        public async Task DeleteAsync(Delete toDelete)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                int tableId = await Tables.GetIdAsync(Ctxt, toDelete.table, noCreate: true, noException: true).ConfigureAwait(false);
                if (tableId < 0)
                    return;

                foreach (var val in toDelete.values)
                {
                    long valueId = await Values.GetIdAsync(Ctxt, val).ConfigureAwait(false);
                    string sql = $"DELETE FROM items WHERE valueid = {valueId} AND tableid = {tableId}";
                    Ctxt.AddPostOp(sql);
                }

                await Ctxt.ProcessPostOpsAsync().ConfigureAwait(false);
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.Delete", totalTimer);
            }
        }

        /// <summary>
        /// Drop a table from the database schema
        /// </summary>
        /// <param name="table">Name of table to drop</param>
        public async Task DropAsync(string table)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                NameValues.ClearCaches();

                int tableId = await Tables.GetIdAsync(Ctxt, table, noCreate: true, noException: true).ConfigureAwait(false);
                if (tableId < 0)
                    return;

                await Ctxt.Db.ExecuteSqlAsync($"DELETE FROM itemnamevalues WHERE nameid IN (SELECT id FROM names WHERE tableid = {tableId})").ConfigureAwait(false);
                await Ctxt.Db.ExecuteSqlAsync($"DELETE FROM names WHERE tableid = {tableId}").ConfigureAwait(false);
                await Ctxt.Db.ExecuteSqlAsync($"DELETE FROM items WHERE tableid = {tableId}").ConfigureAwait(false);
                await Ctxt.Db.ExecuteSqlAsync($"DELETE FROM tables WHERE id = {tableId}").ConfigureAwait(false);

                NameValues.ClearCaches();
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.Drop", totalTimer);
            }
        }

        /// <summary>
        /// Reset the metastrings database
        /// Only used internally for testing, should not be used in a production environment
        /// </summary>
        /// <param name="reset">Reset request object</param>
        public void Reset(bool includeNameValues = false)
        {
            if (includeNameValues)
                NameValues.Reset(Ctxt);
            else
                Items.Reset(Ctxt);

            NameValues.ClearCaches();
        }

        /// <summary>
        /// Get the schema of a metastrings database
        /// </summary>
        /// <param name="table">Name of table to get the schema of</param>
        /// <returns>Schema object</returns>
        public async Task<SchemaResponse> GetSchemaAsync(string table)
        {
            string sql =
                "SELECT t.name AS tablename, n.name AS colname " +
                "FROM tables t JOIN names n ON n.tableid = t.id";

            string requestedTable = table;
            bool haveRequestedTableName = !string.IsNullOrWhiteSpace(requestedTable);
            if (haveRequestedTableName)
                sql += " WHERE t.name = @name";
            
            sql += " ORDER BY tablename, colname";

            Dictionary<string, object> cmdParams = new Dictionary<string, object>();
            if (haveRequestedTableName)
                cmdParams.Add("@name", requestedTable);

            var responseDict = new ListDictionary<string, List<string>>();
            using (var reader = await Ctxt.Db.ExecuteReaderAsync(sql, cmdParams).ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    string curTable = reader.GetString(0);
                    string colname = reader.GetString(1);

                    if (!responseDict.ContainsKey(curTable))
                        responseDict.Add(curTable, new List<string>());

                    responseDict[curTable].Add(colname);
                }
            }

            SchemaResponse response = new SchemaResponse() { tables = responseDict };
            return response;
        }

        /// <summary>
        /// Explicitly create a table in the schema.
        /// This is usually unnecessary as tables are created as referred to by Define.
        /// </summary>
        /// <param name="name">Table name to create request</param>
        public async Task CreateTableAsync(string name, bool isNumeric)
        {
            await Tables.GetIdAsync(Ctxt, name, isNumeric).ConfigureAwait(false);
        }

        /// <summary>
        /// Put a long strings into the database
        /// </summary>
        /// <param name="put">String put request</param>
        public async Task PutLongStringAsync(LongStringPut put)
        {
            await LongStrings.StoreStringAsync(Ctxt, put.itemId, put.fieldName, put.longString).ConfigureAwait(false);
        }

        /// <summary>
        /// Get a long string from the database
        /// </summary>
        /// <param name="get">String get request</param>
        /// <returns>Long string value, or null if not found</returns>
        public async Task<string> GetLongStringAsync(LongStringOp get)
        {
            string longString = await LongStrings.GetStringAsync(Ctxt, get.itemId, get.fieldName).ConfigureAwait(false);
            return longString;
        }

        /// <summary>
        /// Remove a long strings from the database
        /// </summary>
        /// <param name="del">String deletion request</param>
        public async Task DeleteLongStringAsync(LongStringOp del)
        {
            await LongStrings.DeleteStringAsync(Ctxt, del.itemId, del.fieldName).ConfigureAwait(false);
        }

        private Context Ctxt;
    }
}
