using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Text;
using static Slapper.AutoMapper;

namespace Massive
{
    /// <summary>
    /// Adding some features to Massive ORM.
    /// </summary>
    public partial class DynamicModel
    {
        public delegate void MapperCallback<TMainObject, TLinkedObjects>(TMainObject mainObject, TLinkedObjects[] linkedObjects);

        private const string SPLITTER_COLUMN = "___";

        /// <summary>
        /// Searches for a record based on its primary key.
        /// </summary>
        /// <param name="id">The PK value</param>
        /// <returns>The found object or null if no object is found</returns>
        public T GetByPrimaryKey<T>(object id) where T : class
        {
            dynamic result = Single(id);
            return result == null ? null : (T)MapDynamic<T>(result);
        }

        /// <summary>
        /// Statically-typed version of <seealso cref="All(string, string, int, string, object[])"/> method.
        /// </summary>
        /// <returns>Streaming enumerable with objects of type T, one for each row read</returns>
        public IEnumerable<T> All<T>(string where = "", string orderBy = "", int limit = 0, string columns = "*", params object[] args)
        {
            return MapDynamic<T>(All(where, orderBy, limit, columns, args));
        }

        /// <summary>
        /// Statically-typed version of <seealso cref="Single(string, object[])"/> method.
        /// </summary>
        /// <returns>The first record that matches the predicate or null if nothing is found</returns>
        public T Single<T>(string where, params object[] args) where T : class
        {
            dynamic result = Single(where, args);
            return result == null ? null : (T)MapDynamic<T>(result);
        }

        /// <summary>
        /// Statically-typed version of <seealso cref="Paged(string, string, string, string, string, int, int, object[])"/> method.
        /// </summary>
        /// <returns>
        /// An object of type ValueTuple containing total records, total pages and items in the current page.
        /// NOTE: For this method to compile you need C# 7.0 compiler (VS2017) and System.ValueTuple package
        /// (can be added via NuGet).
        /// </returns>
        public (int TotalRecords, int TotalPages, IEnumerable<T> Items) Paged<T>(string sql, string primaryKey, string where = "", string orderBy = "", string columns = "*", int pageSize = 20, int currentPage = 1, params object[] args)
        {
            var result = Paged(sql, primaryKey, where, orderBy, columns, pageSize, currentPage, args);
            return (
                TotalRecords: result.TotalRecords,
                TotalPages: result.TotalPages,
                Items: ((IEnumerable<dynamic>)result.Items).ToStaticType<T>()
            );
        }

        /// <summary>
        /// Statically-typed version of <seealso cref="Query{T}(string, object[])"/> method.
        /// </summary>
        /// <returns>Streaming enumerable with expandos, one for each row read</returns>
        public IEnumerable<T> Query<T>(string sql, params object[] args)
        {
            foreach (dynamic item in Query(sql, args))
                yield return MapDynamic<T>(item);
        }

        /// <summary>
        /// Statically-typed version of <see cref="Query{T}(string, DbConnection, object[])"/> method.
        /// </summary>
        /// <returns>Streaming enumerable with objects of type T, one for each row read</returns>
        public IEnumerable<T> Query<T>(string sql, DbConnection connection, params object[] args)
        {
            foreach (dynamic item in Query(sql, connection, args))
                yield return MapDynamic<T>(item);
        }

        /// <summary>
        /// Queries the database and allows you to map each record to multiple objects. This is used
        /// to fetch associations by using only one round trip to database.
        /// <br/>
        /// NOTE: If you are familiar with Dapper.Net, this is what multi mapping feature does.
        /// </summary>
        /// <param name="sql">The SQL query to be executed</param>
        /// <param name="mapper">The callback that is called for each record to make associations. The first
        /// parameter of the callback is the main object and the second one is a list of associated objects
        /// </param>
        /// <param name="useSplitter">Determines how objects must be splitted in rows. If false is passed,
        /// objects are splitted by columns with name of "id" (case-insesitive).
        /// If true is passed, your query must return some special columns (AKA splitter columns) that delimit
        /// columns of each individual object. To specify splitter columns have their names start with <c>SPLITTER_COLUMN</c></param>
        /// <param name="args">The parameter values</param>
        /// <returns>Streaming enumerable with expandos, one for each row read</returns>
        public IEnumerable<dynamic> QueryAndLink(string sql, MapperCallback<dynamic, dynamic> mapper, bool useSplitter, params object[] args)
        {
            int linkedObjectsCount = 19;            // I think the maximum objects returned from a record is 20 (1 main object and 19 linked objects). If you need more you can change it here.
            dynamic mainObject = null;              // The main object that is read from each record.
            object[] linkedObjects = new object[linkedObjectsCount];     // List of the objects that are read from each record.
            int objectIndex;                        // Index of the last item in "linkedObjects".

            using (DbConnection con = OpenConnection())
            {
                DbCommand cmd = CreateCommand(sql, con, args);
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        // Resetting stuffs.
                        mainObject = null;
                        Array.Clear(linkedObjects, 0, linkedObjectsCount);
                        objectIndex = 0;
                        IDictionary<string, object> obj = null;

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string fieldName = reader.GetName(i);

                            if (useSplitter && fieldName.StartsWith(SPLITTER_COLUMN))
                            {
                                // A splitter column is found. Columns must be mapped to a new object from now on.
                                obj = new ExpandoObject();
                                linkedObjects[objectIndex++] = obj;
                                continue;
                            }
                            else if (!useSplitter && fieldName.Equals("id", StringComparison.OrdinalIgnoreCase))
                            {
                                // An "Id" column is found. Columns must be mapped to a new object from now on.
                                obj = new ExpandoObject();
                                linkedObjects[objectIndex++] = obj;
                            }
                            else if (i == 0)
                            {
                                /* It is the first column that was retrieved (meaning it is the begining of the first object in the record).
                                 * Columns must be mapped to a new object from now on. */
                                obj = new ExpandoObject();
                                mainObject = obj;
                            }

                            object temp = reader.GetValue(i);
                            obj[fieldName] = (temp == DBNull.Value ? null : temp);
                        }

                        mapper.Invoke(mainObject, linkedObjects);
                        yield return mainObject;
                    }
                }
            }
        }

        /// <summary>
        /// Statically-typed version of <seealso cref="QueryAndLink(string, MapperCallback{dynamic, dynamic}, bool, object[])"/> method.
        /// </summary>
        /// <returns>Streaming enumerable with objects of type T, one for each row read</returns>
        public IEnumerable<T> QueryAndLink<T>(string sql, MapperCallback<T, object> mapper, Type[] linkedTypes, bool useSplitter, params object[] args) where T : class, new()
        {
            int linkedObjectsCount = linkedTypes.Length;
            T mainObject = null;                        // The main object that is read from each record.
            object[] linkedObjects = new object[linkedObjectsCount];    // List of the linked objects that are read from each record.
            int objectIndex;                            // Index of the last item in "linkedObjects".
            IEnumerator typeEnumerator = linkedTypes.GetEnumerator();
            Type objectType = null;

            using (DbConnection con = OpenConnection())
            {
                DbCommand cmd = CreateCommand(sql, con, args);
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        // Resetting stuffs.
                        mainObject = null;
                        Array.Clear(linkedObjects, 0, linkedObjectsCount);
                        objectIndex = 0;
                        object obj = null;
                        typeEnumerator.Reset();

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string fieldName = reader.GetName(i);

                            if (useSplitter && fieldName.StartsWith(SPLITTER_COLUMN))
                            {
                                // A splitter column is found. Columns must be mapped to a new object from now on.
                                typeEnumerator.MoveNext();
                                objectType = (Type)typeEnumerator.Current;
                                obj = Activator.CreateInstance(objectType);
                                linkedObjects[objectIndex++] = obj;
                                continue;
                            }
                            else if (!useSplitter && fieldName.Equals("id", StringComparison.OrdinalIgnoreCase))
                            {
                                // An "Id" column is found. Columns must be mapped to a new object from now on.
                                typeEnumerator.MoveNext();
                                objectType = (Type)typeEnumerator.Current;
                                obj = Activator.CreateInstance(objectType);
                                linkedObjects[objectIndex++] = obj;
                            }
                            else if (i == 0)
                            {
                                /* It is the first column that was retrieved (meaning it is the begining of the first object in the record).
                                 * Columns must be mapped to a new object from now on. */
                                objectType = typeof(T);
                                mainObject = new T();
                                obj = mainObject;
                            }

                            object temp = reader.GetValue(i);
                            objectType.GetProperty(fieldName)?.SetValue(obj, (temp == DBNull.Value ? null : temp));
                        }

                        mapper.Invoke(mainObject, linkedObjects);
                        yield return mainObject;
                    }
                }
            }
        }

        /// <summary>
        /// Enables you to insert POCO objects (which contain a property for primary key) instead of dynamic object.
        /// If the object owns a property for its PK, Massive <c>Insert</c> method tries to insert it into the database
        /// (along with other properties) and this operation fails when PK column is auto increment in the database.
        /// This method stops the PK property from being inserted.
        /// </summary>
        /// <param name="o">The POCO object to be inserted</param>
        /// <returns>The inserted object returned by <see cref="Insert(object)"/>
        /// </returns>
        public dynamic InsertPoco(object o)
        {
            var oAsExpando = o.ToExpando();
            var oAsDictionary = (IDictionary<string, object>)oAsExpando;
            oAsDictionary.Remove(PrimaryKeyField);
            dynamic result = Insert(oAsDictionary);
            o.GetType().GetProperty(PrimaryKeyField).SetValue(o, oAsDictionary[PrimaryKeyField]);
            return result;
        }

        /// <summary>
        /// Perform a batch insert operation. Unlike <c>Save</c> which sends a separate command for every object
        /// to the database, this method sends a single command.
        /// Also <c>BeforeSave</c> and <c>Inserted</c> are called once for the entire batch.
        /// </summary>
        /// <param name="batch">The objects to be inserted</param>
        /// <returns>Number of inserted objects</returns>
        public int InsertBatch(params object[] batch)
        {
            int result = 0;
            ICollection<IDictionary<string, object>> dict = batch.ToDictionary();    // Converting batch items to dictionary (ExpandoObject).

            if (!IsValid(dict))
                throw new InvalidOperationException("Can't insert: " + string.Join("; ", Errors.ToArray()));

            if (BeforeSave(dict))
            {
                using (DbConnection con = OpenConnection())
                {
                    using (DbTransaction tran = con.BeginTransaction())
                    {
                        bool includePrimaryKeyInSql = false;
                        if (_sequenceValueCallsBeforeMainInsert && !string.IsNullOrEmpty(_primaryKeyFieldSequence))
                        {
                            DbCommand sequenceCmd = CreateCommand(GetIdentityRetrievalScalarStatement(), con);
                            sequenceCmd.Transaction = tran;
                            foreach (var item in dict)
                                item[PrimaryKeyField] = Convert.ToInt32(sequenceCmd.ExecuteScalar());
                            includePrimaryKeyInSql = true;
                        }

                        DbCommand cmd = CreateInsertCommandForBatch(dict, con, includePrimaryKeyInSql);
                        cmd.Transaction = tran;
                        result = cmd.ExecuteNonQuery();

                        tran.Commit();
                    }
                }

                Inserted(dict);
            }

            return result;
        }

        /// <summary>
        /// Perform a batch update operation. Unlike <c>Save</c> which sends a separate command for every object
        /// to the database, this method sends a single command
        /// Also <c>BeforeSave</c> and <c>Updated</c> are called once for the entire batch.
        /// </summary>
        /// <param name="batch">The objects to be updated</param>
        /// <returns>Number of updated objects</returns>
        public int UpdateBatch(params object[] batch)
        {
            int result = 0;
            ICollection<IDictionary<string, object>> dict = batch.ToDictionary();    // Converting batch items to dictionary (ExpandoObject).

            if (!IsValid(dict))
                throw new InvalidOperationException("Can't Update: " + string.Join("; ", Errors.ToArray()));

            if (BeforeSave(dict))
            {
                using (DbConnection con = OpenConnection())
                {
                    using (DbTransaction tran = con.BeginTransaction())
                    {
                        DbCommand cmd = CreateUpdateCommandForBatch(dict, con);
                        cmd.Transaction = tran;
                        result = Convert.ToInt32(cmd.ExecuteNonQuery());
                        tran.Commit();
                    }
                }

                Updated(dict);
            }

            return result;
        }

        /// <summary>
        /// Perform a batch delete operation. <c>BeforeDelete</c> and <c>Deleted</c> are called once for the entire batch.
        /// </summary>
        /// <param name="batch">The objects to be deleted</param>
        /// <returns>Number of deleted objects</returns>
        public int DeleteBatch(params object[] batch)
        {
            int result = 0;
            ICollection<IDictionary<string, object>> dict = batch.ToDictionary();    // Converting batch items to dictionary (ExpandoObject).

            if (BeforeDelete(dict))
            {
                using (DbConnection con = OpenConnection())
                {
                    using (DbTransaction tran = con.BeginTransaction())
                    {
                        DbCommand cmd = CreateDeleteCommandForBatch(dict, con);
                        cmd.Transaction = tran;
                        result = Convert.ToInt32(cmd.ExecuteNonQuery());
                        Deleted(dict);
                        tran.Commit();
                    }
                }

                Deleted(dict);
            }

            return result;
        }

        #region Helper Methods

        /// <summary>
        /// Creates a command object for performing a batch insert operation.
        /// </summary>
        /// <param name="batch">A collection of items to be insered (in the form of Dictionary / ExpandoObjects)</param>
        /// <param name="connectionToUse">A live connection to the database</param>
        /// <param name="includePrimaryKey">The PK proeprty is inserted nro no</param>
        /// <returns>The created command object</returns>
        private DbCommand CreateInsertCommandForBatch(ICollection<IDictionary<string, object>> batch, DbConnection connectionToUse, bool includePrimaryKey)
        {
            DbCommand result = CreateCommand(null, connectionToUse);
            StringBuilder sql = new StringBuilder();
            var fieldNames = new List<string>();
            var valueParameters = new List<string>();
            string insertQueryPattern = GetInsertQueryPattern() + ";";
            int counter = 0;

            foreach (KeyValuePair<string, object> item in batch.First())
            {
                if (!includePrimaryKey && item.Key.Equals(PrimaryKeyField, StringComparison.OrdinalIgnoreCase)) continue;
                fieldNames.Add(item.Key);
            }

            if (fieldNames.Count > 0)
            {
                foreach (IDictionary<string, object> obj in batch)
                {
                    foreach (KeyValuePair<string, object> item in obj)
                    {
                        if (!includePrimaryKey && item.Key.Equals(PrimaryKeyField, StringComparison.OrdinalIgnoreCase)) continue;
                        valueParameters.Add(PrefixParameterName(counter.ToString()));
                        result.AddParam(item.Value);
                        counter++;
                    }
                    sql.Append(string.Format(insertQueryPattern, TableName, string.Join(", ", fieldNames.ToArray()), string.Join(", ", valueParameters.ToArray())));
                    valueParameters.Clear();
                }
            }
            else
            {
                throw new InvalidOperationException("Can't parse this object to the database - there are no properties set");
            }

            result.CommandText = sql.ToString();
            return result;
        }

        /// <summary>
        /// Creates a command object for performing a batch update operation
        /// </summary>
        /// <param name="batch">A collection of items to be updated (in the form of Dictionary / ExpandoObjects)</param>
        /// <param name="connectionToUse">A live connection to the database</param>
        /// <returns>The created command object</returns>
        private DbCommand CreateUpdateCommandForBatch(ICollection<IDictionary<string, object>> batch, DbConnection connectionToUse)
        {
            DbCommand result = CreateCommand(null, connectionToUse);
            StringBuilder sql = new StringBuilder();
            var fieldSetFragments = new List<string>();
            var updateQueryPattern = GetUpdateQueryPattern() + $"WHERE {PrimaryKeyField} = {{2}};";
            int counter = 0;

            foreach (IDictionary<string, object> obj in batch)
            {
                foreach (KeyValuePair<string, object> item in obj)
                {
                    var val = item.Value;
                    if (!item.Key.Equals(PrimaryKeyField, StringComparison.OrdinalIgnoreCase))
                    {
                        if (item.Value == null)
                        {
                            fieldSetFragments.Add(string.Format("{0} = NULL", item.Key));
                        }
                        else
                        {
                            result.AddParam(val);
                            fieldSetFragments.Add(string.Format("{0} = {1}", item.Key, PrefixParameterName(counter.ToString())));
                            counter++;
                        }
                    }
                }
                if (fieldSetFragments.Count > 0)
                {
                    sql.Append(string.Format(updateQueryPattern, TableName, string.Join(", ", fieldSetFragments.ToArray()), obj[PrimaryKeyField]));
                }
                else
                {
                    throw new InvalidOperationException("No parsable object was sent in - could not define any name/value pairs");
                }

                fieldSetFragments.Clear();
            }

            result.CommandText = sql.ToString();
            return result;
        }

        /// <summary>
        /// Creates a command object for performing a batch delete operation.
        /// </summary>
        /// <param name="batch">A collection of items to be deleted (in the form of Dictionary / ExpandoObjects)</param>
        /// <param name="connectionToUse">A live connection to the database</param>
        /// <returns>The created command object</returns>
        private DbCommand CreateDeleteCommandForBatch(ICollection<IDictionary<string, object>> batch, DbConnection connectionToUse)
        {
            DbCommand result = CreateCommand(null, connectionToUse);
            StringBuilder sql = new StringBuilder();
            var deleteQueryPattern = string.Format(GetDeleteQueryPattern(), TableName) + $"WHERE {PrimaryKeyField} IN ({{0}});";
            int counter = 0;

            foreach (IDictionary<string, object> obj in batch)
            {
                result.AddParam(obj[PrimaryKeyField]);
                sql.Append(PrefixParameterName(counter.ToString() + ","));
                counter++;
            }
            if (sql.Length > 0) sql.Remove(sql.Length - 1, 1);

            result.CommandText = string.Format(deleteQueryPattern, sql.ToString());
            return result;
        }

        #endregion Helper Methods
    }

    public static partial class ObjectExtensions
    {
        /// <summary>
        /// Converts a batch of objects to a collection of Dinctionaries (ExpoandObjects).
        /// </summary>
        /// <param name="batch">The input objects to be converted</param>
        /// <returns>A collection of converted objects</returns>
        public static ICollection<IDictionary<string, object>> ToDictionary(this IEnumerable<object> batch)
        {
            ICollection<IDictionary<string, object>> result = new LinkedList<IDictionary<string, object>>();
            foreach (var item in batch) result.Add(item.ToExpando());
            return result;
        }

        /// <summary>
        /// Converts a batch of dynamic objects to an enumeration of static object of type T.
        /// </summary>
        /// <typeparam name="T">Type of the returning object</typeparam>
        /// <param name="batch">The input batch of dynamic objects to be converted</param>
        /// <returns>Streaming enumerable with objects of type T, one for each item in the batch</returns>
        public static IEnumerable<T> ToStaticType<T>(this IEnumerable<dynamic> batch)
        {
            foreach (dynamic item in batch)
                yield return MapDynamic<T>(item);
        }
    }
}