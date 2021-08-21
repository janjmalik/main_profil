using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace metastrings
{
    /// <summary>
    /// Manage long strings, keeping them out of the bvalues SQL table,
    /// allowing bvalues to have a UNIQUE index, ensuring database integrity
    /// </summary>
    public static class LongStrings
    {
        internal static string[] CreateSql
        {
            get
            {
                return new[]
                {
                    "CREATE TABLE longstrings\n(\n" +
                    "itemId INTEGER NOT NULL,\n" +
                    "name TEXT NOT NULL,\n" +
                    "longstring TEXT NOT NULL,\n" +
                    "PRIMARY KEY (itemid, name),\n" +
                    "FOREIGN KEY(itemId) REFERENCES items(id)\n" +
                    ")"
                };
            }
        }

        /// <summary>
        /// Given an item and a name, save a long string.
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="itemId">Item to store the string for</param>
        /// <param name="name">Name of the string</param>
        /// <param name="longstring">String value</param>
        public static async Task StoreStringAsync(Context ctxt, long itemId, string name, string longstring)
        {
            if (longstring.Length >= 64 * 1024)
                throw new MetaStringsException("String length exceeds max 64KB");

            var cmdParams =
                new Dictionary<string, object>
                {
                    { "@itemid", itemId },
                    { "@name", name },
                    { "@longstring", longstring }
                };
            string updateSql =
                "UPDATE longstrings SET longstring = @longstring WHERE itemid = @itemid AND name = @name";
            int affected = await ctxt.Db.ExecuteSqlAsync(updateSql, cmdParams).ConfigureAwait(false);
            if (affected == 0)
            {
                string insertSql =
                    "INSERT INTO longstrings (itemid, name, longstring)\n" +
                    "VALUES (@itemid, @name, @longstring)";
                await ctxt.Db.ExecuteSqlAsync(insertSql, cmdParams).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Given an item and a name and optionally a LIKE query, and try to get a long string
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="itemId">The item to look in</param>
        /// <param name="name">The name to match</param>
        /// <returns>Long string value if found, null otherwise</returns>
        public static async Task<string> GetStringAsync(Context ctxt, long itemId, string name)
        {
            var cmdParams =
                new Dictionary<string, object>
                {
                    { "@itemid", itemId },
                    { "@name", name }
                };
            string storeSql = "SELECT longstring FROM longstrings WHERE itemid = @itemid AND name = @name";
            using (var reader = await ctxt.Db.ExecuteReaderAsync(storeSql, cmdParams).ConfigureAwait(false))
            {
                if (!await reader.ReadAsync().ConfigureAwait(false))
                    return null;
                else
                    return reader.GetString(0);
            }
        }

        /// <summary>
        /// Remove a long string for the database
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="itemId">The item</param>
        /// <param name="name">The name</param>
        public static async Task DeleteStringAsync(Context ctxt, long itemId, string name)
        {
            var cmdParams =
                new Dictionary<string, object>
                {
                    { "@itemid", itemId },
                    { "@name", name },
                };
            string deleteSql = "DELETE FROM longstrings WHERE itemid = @itemid AND name = @name";
            await ctxt.Db.ExecuteSqlAsync(deleteSql, cmdParams).ConfigureAwait(false);
        }
    }
}
