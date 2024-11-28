using Microsoft.Extensions.Hosting;
using Mono.Unix.Native;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;

namespace OGA.Postgres
{
    public class Postgres_DAL : IDisposable
    {
        #region Private Fields

        static private string _classname = nameof(Postgres_DAL);

        static private volatile int _instancecounter;

        private string _connstring;
        private bool _explicit_ConnectionOpen_Called;

#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, this DAL leaves it out when compiled for NET5.
#else
        private NpgsqlDataSource _dbDSource = null;
#endif
        private NpgsqlConnection _dbConnection = null;

        private bool disposedValue;

#endregion


        #region Properties

        public string Hostname { get; set; }
        public string Database { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        #endregion


        #region ctor / dtor

        public Postgres_DAL()
        {
            Hostname = "";
            Database = "";
            Username = "";
            Password = "";
            _connstring = "";

            _explicit_ConnectionOpen_Called = false;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                this.Disconnect();

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~Postgres_DAL()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion


        #region Connection Management

        /// <summary>
        /// Caller wants a persistent connection for multiple database calls.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <returns></returns>
        public int Connect()
        {
            if(this.disposedValue)
            {
                // Already disposed.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Connect)} - " +
                    "Already disposed.");

                return -1;
            }

            // Attempt to create a persistent connection.
            return Connect(true);
        }
        /// <summary>
        /// Private connect method responsible for all connection work.
        /// Accepts a flag to know if called by the public Connect() method, or from a query method.
        /// </summary>
        /// <param name="calledfrompublicconnect"></param>
        /// <returns></returns>
        protected int Connect(bool calledfrompublicconnect)
        {
            // Check if the connection is already open...
            if(_dbConnection != null)
            {
                // We already have a connection instance.

                // Check if it's viable...
                if(_dbConnection.State == System.Data.ConnectionState.Connecting ||
                    _dbConnection.State == System.Data.ConnectionState.Executing ||
                    _dbConnection.State == System.Data.ConnectionState.Fetching ||
                    _dbConnection.State == System.Data.ConnectionState.Open)
                {
                    // The connection is open and viable.
                    // We will accept it as good.

                    // Mark the connection as persistent if needed...
                    if(calledfrompublicconnect)
                        this._explicit_ConnectionOpen_Called = true;

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                        $"{_classname}:-:{nameof(Connect)} - " +
                        "Connection already open to PostgreSQL server.");

                    return 1;
                }
                // The connection is broken or closed.
                // We will close it and reconnect.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(Connect)} - " +
                    "Existing connection is not viable. Closing existing connection...");

                // Close the connection, so we can reconnect...
                this.Disconnect();
            }
            // If here, we have no existing connection.
            // We will attempt to stand one up.

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(Connect)} - " +
                    "Attempting to open a connection to PostgreSQL server...");

                // Attempt to create a connection...
                var res = this.CreateConnection();
                if(res.res != 1 || res.conn == null)
                {
                    // Connection failed.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Connect)} - " +
                        "An error occurred while connecting to the database.");

                    return -1;
                }
                // If here, we were able to open a connection.

                // Accept the created connection and datasource...
                this._dbConnection = res.conn;
#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, this DAL leaves it out when compiled for NET5.
#else
                this._dbDSource = res.dsource;
#endif

                // See if we were called to make a persistent connection...
                if(calledfrompublicconnect)
                {
                    // Caller wants a persistent connection.
                    this._explicit_ConnectionOpen_Called = true;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(Connect)} - " +
                    "Connection opened to PostgreSQL server.");

                // If here, we have a connection we can use to query for data.
                return 1;
            }
            catch (Exception e)
            {
                // Something went wrong while attempting to connect to the database.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Connect)} - " +
                    "An exception was caught while connecting to the database.");

                this.Disconnect();

                return -2;
            }
            finally
            {
            }
        }

        public int Disconnect()
        {
            OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                $"{_classname}:-:{nameof(Disconnect)} - " +
                "Attempting to close and disposed database connection...");

            // Cleanup the database connection.
            try
            {
                this._dbConnection?.Close();
            }
            catch (Exception) { }
            try
            {
                this._dbConnection?.Dispose();
            }
            catch (Exception) { }

            this._dbConnection = null;

#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, this DAL leaves it out when compiled for NET5.
#else
            try
            {
                this._dbDSource?.Dispose();
            }
            catch (Exception) { }

            this._dbDSource = null;
#endif

            _explicit_ConnectionOpen_Called = false;

            OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                $"{_classname}:-:{nameof(Disconnect)} - " +
                "Database connection closed and disposed.");

            return 1;
        }

        /// <summary>
        /// Attempt to compose the connection string.
        /// Returns negatives for failure.
        /// </summary>
        /// <returns></returns>
        public int Create_ConnectionString()
        {
            // PostGres compliant connection strings are of the form:
            //  "Host=my_host;Database=my_db;Username=my_user;Password=my_pw"

            // Check that all necessary values are set.
            if (Hostname == "")
            {
                // Unable to generate connection string without host name.
                return -1;
            }
            if (Database == "")
            {
                // Unable to generate connection string without database name.
                return -1;
            }
            if (Username == "")
            {
                // Unable to generate connection string without host name.
                return -1;
            }
            if (Password == "")
            {
                // Unable to generate connection string without host name.
                return -1;
            }
            // If here, we have all values necessary to format a connection string.

            // Piece together the connection string.
            this._connstring = "Host=" + Hostname + ";Database=" + Database + ";Username=" + Username + ";Password=" + Password;

            // Success
            return 1;
        }

        public string Get_ConnectionString()
        {
            return _connstring;
        }

        /// <summary>
        /// Returns 1 for valid connection, negatives for error.
        /// </summary>
        /// <returns></returns>
        public int Test_Connection()
        {
            if(this.disposedValue)
            {
                // Already disposed.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Test_Connection)} - " +
                    "Already disposed.");

                return -1;
            }

            try
            {
                // Check if we already have an open connection...
                if(this._explicit_ConnectionOpen_Called &&
                    this._dbConnection != null &&
                    this._dbConnection.State == System.Data.ConnectionState.Open)
                {
                    // The connection is already open.
                    // We will simply return success.

                    return 1;
                }
                // No connection exists.

                // Attempt to create a connection...
                var res = this.CreateConnection();
#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, we don't check that it was created.
                if(res.res != 1 || res.conn == null)
#else
                if(res.res != 1 || res.conn == null || res.dsource == null)
#endif
                {
                    // Connection failed.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Test_Connection)} - " +
                        "An error occurred while connecting to the database.");

                    return -1;
                }
                // If here, we were able to open a connection.

                // Return success to the caller.
                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Test_Connection)} - " +
                    "An exception occurred while attempting to connect to the database.");

                return -2;
            }
            finally
            {
                if(!this._explicit_ConnectionOpen_Called)
                    this.Disconnect();
            }
        }

        /// <summary>
        /// Private method that does the actual connection creation.
        /// This was done, to consolidate connection creation logic, for easier management.
        /// </summary>
        /// <returns></returns>
#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, the return signature doesn't include it.
        private (int res, Npgsql.NpgsqlConnection? conn) CreateConnection()
#else
        private (int res, NpgsqlDataSource dsource, Npgsql.NpgsqlConnection? conn) CreateConnection()
#endif
        {
            bool success = false;
#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
#else
            NpgsqlDataSource? ds = null;
#endif
            NpgsqlConnection? conn = null;

            // See if the connection string has been set.
            if (string.IsNullOrEmpty(_connstring))
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(CreateConnection)} - " +
                    "Attempting to setup Postgres connection string...");

                // No connection string was defined.
                // Attempt to put it together.
                if (this.Create_ConnectionString() < 0)
                {
                    // An error occurred while piecing together the connection string.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(CreateConnection)} - " +
                        "An error occurred while piecing together the connection string.");

#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, the return signature doesn't include it.
                    return (-1, null);
#else
                    return (-1, null, null);
#endif
                }
                // If here, we have a connection string.
            }
            // If here, we have the connection string.

            OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                $"{_classname}:-:{nameof(CreateConnection)} - " +
                "PostgreSQL connection string composed for use.");

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(CreateConnection)} - " +
                    "Attempting to open a connection to Postgres server...");

#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, we directly create the connection instance in NET5.
                conn = new Npgsql.NpgsqlConnection(this._connstring);
                conn.Open();
#else

                // Create a datasource...
                ds = NpgsqlDataSource.Create(this._connstring);

                // Open a connection...
                conn = ds.OpenConnection();
#endif

                // Wait for it to respond as Open...
                var res = OGA.Common.Process.cRuntime_Helpers.WaitforCondition(() =>
                    conn.State == System.Data.ConnectionState.Open, 1000).GetAwaiter().GetResult();
                if(res != 1)
                {
                    // Timed out waiting for open state.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(CreateConnection)} - " +
                        "Connection failed to reach Open state.");

#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, the return signature doesn't include it.
                    return (-2, null);
#else
                    return (-2, null, null);
#endif
                }
                // If here, the connection reads as Open.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(CreateConnection)} - " +
                    "Connection opened to Postgres server.");

                success = true;

                // If here, we have a connection we can use to query for data.
#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, the return signature doesn't include it.
                return (1, conn);
#else
                return (1, ds, conn);
#endif
            }
            catch (Exception e)
            {
                // Something went wrong while attempting to connect to the database.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(CreateConnection)} - " +
                    "An exception was caught while connecting to the database.");

#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
    // So, the return signature doesn't include it.
                return (-2, null);
#else
                return (-2, null, null);
#endif
            }
            finally
            {
                if(!success)
                {
                    try
                    {
                        conn?.Close();
                    } catch(Exception) { }
                    try
                    {
                        conn?.Dispose();
                    } catch(Exception) { }

                    conn = null;

#if (NET5 || NET6)
    // The NET5 and NET6 releases of NPGSQL doesn't include the NpgsqlDataSource type.
#else
                    try
                    {
                        ds?.Dispose();
                    } catch(Exception) { }

                    ds = null;
#endif
                }
            }
        }

        #endregion


        #region Query Methods

        /// <summary>
        /// Call this method to execute a stored procedure that returns an RC integer, but no terminal select.
        /// Returns 1 for success. Negatives for error.
        /// The return includes affected rows and the RC return val.
        /// </summary>
        /// <param name="sprocname"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        public (int res, int affectrowcount, int RCVal) Execute_SProc_accepting_ParameterList_withReturnIntVal(string sprocname, List<NpgsqlParameter> arguments)
        {
            int result = 0;
            int returnvalue = -9999;
            NpgsqlCommand cmd = null;

            if(this.disposedValue)
            {
                // Already disposed.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Execute_SProc_accepting_ParameterList_withReturnIntVal)} - " +
                    "Already disposed.");

                return (-1, 0, 0);
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(Execute_SProc_accepting_ParameterList_withReturnIntVal)} - " +
                    "Attempting to call stored procedure call accepting multiple parameters:\r\n" +
                    "sprocname = " + sprocname + "\r\n");

                // We need a database connection for this query.
                // See if we have an existing one...
                var resconn = this.Connect(false);
                if(resconn != 1)
                {
                    // Failed to open database connection.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Execute_SProc_accepting_ParameterList_withReturnIntVal)} - " +
                        "Already disposed.");

                    return (-1, 0, 0);
                }
                // If here, we have a connection we can use.

                try
                {
                    // Set the command to call the desired stored procedure.
                    cmd = new NpgsqlCommand(sprocname, _dbConnection);
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;

                    // Load in arguments for the stored procedure.
                    if(arguments.Count != 0)
                    {
                        // Load all arguments.

                        foreach(var s in arguments)
                        {
                            cmd.Parameters.Add(s);
                        }
                        ////Pass table Valued parameter to Store Procedure
                        //System.Data.SqlClient.SqlParameter sqlParam1 = cmd.Parameters.AddWithValue(table1name, dt1);
                        //sqlParam1.SqlDbType = System.Data.SqlDbType.Structured;
                        //System.Data.SqlClient.SqlParameter sqlParam2 = cmd.Parameters.AddWithValue(table2name, dt2);
                        //sqlParam2.SqlDbType = System.Data.SqlDbType.Structured;
                    }

                    // Create the return value.
                    var retpar = new NpgsqlParameter("@RC", System.Data.SqlDbType.Int);
                    retpar.Direction = System.Data.ParameterDirection.ReturnValue;
                    cmd.Parameters.Add(retpar);

                    // Tell the stored procedure to execute.
                    result = cmd.ExecuteNonQuery();

                    returnvalue = (int)cmd.Parameters["@RC"].Value;
                }
                catch (Exception e)
                {
                    // Something went wrong while attempting to execute the stored procedure.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                        $"{_classname}:-:{nameof(Execute_SProc_accepting_ParameterList_withReturnIntVal)} - " +
                        "An exception was caught while executing the stored procedure, {0}.", sprocname);

                    return (-4, 0, 0);
                }

                // Return success to the caller.
                return (1, result, returnvalue);
            }
            finally
            {
                // Clean up the command.
                this.CloseCommand(cmd);
                cmd = null;

                if(!this._explicit_ConnectionOpen_Called)
                    this.Disconnect();
            }
        }

        /// <summary>
        /// Call this method to execute a stored procedure that includes a terminal select and returns an RC integer.
        /// Returns 1 for success. Negatives for error.
        /// The return includes the RC return val.
        /// </summary>
        /// <param name="sprocname"></param>
        /// <param name="arguments"></param>
        /// <param name="dt"></param>
        /// <returns></returns>
        public (int res, int RCVal) Execute_SProc_accepting_ParameterList_withTerminalSelect_and_ReturnIntVal(string sprocname,
                                        List<NpgsqlParameter> arguments,
                                        out System.Data.DataTable dt)
        {
            int returnvalue = -9999;
            NpgsqlCommand cmd = null;
            NpgsqlDataReader dr = null;
            dt = null;

            if(this.disposedValue)
            {
                // Already disposed.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Execute_SProc_accepting_ParameterList_withTerminalSelect_and_ReturnIntVal)} - " +
                    "Already disposed.");

                return (-1, 0);
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(Execute_SProc_accepting_ParameterList_withTerminalSelect_and_ReturnIntVal)} - " +
                    "Attempting to call stored procedure call accepting multiple parameters:\r\n" +
                    "sprocname = " + sprocname + "\r\n");

                // We need a database connection for this query.
                // See if we have an existing one...
                var resconn = this.Connect(false);
                if(resconn != 1)
                {
                    // Failed to open database connection.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Execute_SProc_accepting_ParameterList_withTerminalSelect_and_ReturnIntVal)} - " +
                        "Already disposed.");

                    return (-1, 0);
                }
                // If here, we have a connection we can use.

                try
                {
                    // Set the command to call the desired stored procedure.
                    cmd = new NpgsqlCommand(sprocname, _dbConnection);
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;

                    // Load in arguments for the stored procedure.
                    if (arguments.Count != 0)
                    {
                        // Load all arguments.

                        foreach (var s in arguments)
                        {
                            cmd.Parameters.Add(s);
                        }
                        ////Pass table Valued parameter to Store Procedure
                        //System.Data.SqlClient.SqlParameter sqlParam1 = cmd.Parameters.AddWithValue(table1name, dt1);
                        //sqlParam1.SqlDbType = System.Data.SqlDbType.Structured;
                        //System.Data.SqlClient.SqlParameter sqlParam2 = cmd.Parameters.AddWithValue(table2name, dt2);
                        //sqlParam2.SqlDbType = System.Data.SqlDbType.Structured;
                    }

                    // Create the return value.
                    var retpar = new NpgsqlParameter("@RC", System.Data.SqlDbType.Int);
                    retpar.Direction = System.Data.ParameterDirection.ReturnValue;
                    cmd.Parameters.Add(retpar);

                    // Tell the stored procedure to execute.
                    dr = cmd.ExecuteReader();
                    dt = new System.Data.DataTable();
                    dt.Load(dr);

                    returnvalue = (int)cmd.Parameters["@RC"].Value;

                    // Return success to the caller.
                    return (1, returnvalue);
                }
                catch (Exception e)
                {
                    // Something went wrong while attempting to execute the stored procedure.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                        $"{_classname}:-:{nameof(Execute_SProc_accepting_ParameterList_withTerminalSelect_and_ReturnIntVal)} - " +
                        "An exception was caught while executing the stored procedure, {0}.", sprocname);

                    // Since the datatable gets returned, we will only clean it up on failure.
                    try
                    {
                        dt?.Dispose();
                    }
                    catch(Exception) { }

                    return (-4, 0);
                }
            }
            finally
            {
                try
                {
                    dr?.Dispose();
                }
                catch(Exception) { }

                // Clean up the command.
                this.CloseCommand(cmd);
                cmd = null;

                if(!this._explicit_ConnectionOpen_Called)
                    this.Disconnect();
            }
        }

        /// <summary>
        /// Provides a means to perform a PostgreSQL COPY IN (using BeginBinaryImporter).
        /// Set the copyargs string to your preamble: COPY tablename (field1, field2, field3) FROM STDIN (FORMAT BINARY)
        /// NOTE: Usage of the BeginBinaryImport call lets us avoid a need to escape/sanitize strings, as each one is converted to binary, instead of raw SQL.
        /// Populate rows with the writer in the callback.
        /// See this article for usage: https://oga.atlassian.net/wiki/spaces/~311198967/pages/211091547/PostgreSQL+Bulk+Insert
        /// </summary>
        /// <param name="copyargs"></param>
        /// <param name="callback"></param>
        /// <returns></returns>
        public int PerformBulkBinaryImport(string copyargs, Action<NpgsqlBinaryImporter> callback)
        {
            NpgsqlBinaryImporter? writer = null;

            if(this.disposedValue)
            {
                // Already disposed.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(PerformBulkBinaryImport)} - " +
                    "Already disposed.");

                return -1;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Trace(
                    $"{_classname}:-:{nameof(PerformBulkBinaryImport)} - " +
                    "Attempting to perform PostGreSQL Copy Import.");

                // We need a database connection for this query.
                // See if we have an existing one...
                var resconn = this.Connect(false);
                if(resconn != 1)
                {
                    // Failed to open database connection.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(PerformBulkBinaryImport)} - " +
                        "Already disposed.");

                    return -1;
                }
                // If here, we have a connection we can use.

                try
                {
                    // Call the caller's lambda...

                    // Create the copy writer...
                    writer = this._dbConnection.BeginBinaryImport(copyargs);

                    // Fill in the caller's table data...
                    callback(writer);

                    // Call complete, to send it to the database...
                    writer.Complete();

                    // Return success to the caller.
                    return 1;
                }
                catch (Exception e)
                {
                    // Something went wrong while attempting to execute the stored procedure.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                        $"{_classname}:-:{nameof(PerformBulkBinaryImport)} - " +
                        "An exception was caught while executing copy import");

                    return -4;
                }
            }
            finally
            {
                try
                {
                    writer?.Dispose();
                }
                catch(Exception) { }

                if(!this._explicit_ConnectionOpen_Called)
                    this.Disconnect();
            }
        }

        /// <summary>
        /// Call to perform a sql query that returns a table.
        /// Returns 1 for success. Negatives for error.
        /// </summary>
        /// <param name="querystring"></param>
        /// <param name="dt"></param>
        /// <returns></returns>
        public int Execute_Table_Query(string querystring, out System.Data.DataTable dt)
        {
            NpgsqlDataAdapter dbAdapter = null;
            NpgsqlCommand cmd = null;
            dt = null;

            if(this.disposedValue)
            {
                // Already disposed.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Execute_Table_Query)} - " +
                    "Already disposed.");

                return -1;
            }

            try
            {
                // We need a database connection for this query.
                // See if we have an existing one...
                var resconn = this.Connect(false);
                if(resconn != 1)
                {
                    // Failed to open database connection.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Execute_Table_Query)} - " +
                        "Already disposed.");

                    return -1;
                }
                // If here, we have a connection we can use.

                try
                {
                    // Make the query to get result data.
                    cmd = new NpgsqlCommand(querystring, this._dbConnection);
                    dbAdapter = new NpgsqlDataAdapter(querystring, _dbConnection);
                }
                catch (Exception e)
                {
                    // Something went wrong while attempting to run the query.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                        $"{_classname}:-:{nameof(Execute_Table_Query)} - " +
                        "An exception was caught while attempting to run the query.");

                    return -3;
                }
                // If here, we have result data from the executed query.

                // Instanciate the datatable reference for usage.
                dt = new System.Data.DataTable();

                try
                {
                    // Get the result data from the query.
                    dbAdapter.Fill(dt);
                }
                catch (Exception e)
                {
                    // Something went wrong while attempting to retrieve result data.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                        $"{_classname}:-:{nameof(Execute_Table_Query)} - " +
                        "An exception was caught while attempting to retrieve result data.");

                    try
                    {
                        dt?.Dispose();
                    } catch(Exception) { }

                    return -4;
                }
                // If here, we have result data.

                // Return success to the caller.
                return 1;
            }
            finally
            {
                // Clean up the adapter.
                try
                {
                    dbAdapter?.Dispose();
                }
                catch (Exception) { }
                dbAdapter = null;

                // Clean up the command.
                this.CloseCommand(cmd);
                cmd = null;

                if(!this._explicit_ConnectionOpen_Called)
                    this.Disconnect();
            }
        }


        /// <summary>
        /// Will execute a scalar command.
        /// Returns 1 for success. Negatives for error.
        /// The return from the sql call will be in the 'output' object reference.
        /// </summary>
        /// <param name="querystring"></param>
        /// <param name="commandtype"></param>
        /// <param name="output"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public int Execute_Scalar(string querystring,
                                    System.Data.CommandType commandtype,
                                    out object output,
                                    NpgsqlParameter[] parameters = null)
        {
            NpgsqlCommand cmd = null;
            output = null;

            if(this.disposedValue)
            {
                // Already disposed.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Execute_Scalar)} - " +
                    "Already disposed.");

                return -1;
            }

            try
            {
                // We need a database connection for this query.
                // See if we have an existing one...
                var resconn = this.Connect(false);
                if(resconn != 1)
                {
                    // Failed to open database connection.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Execute_Scalar)} - " +
                        "Already disposed.");

                    return -1;
                }
                // If here, we have a connection we can use.

                try
                {
                    // Formulate a command.
                    cmd = new NpgsqlCommand(querystring, _dbConnection);

                    // Set the command type.
                    cmd.CommandType = commandtype;

                    // See if any parameters were defined.
                    if (parameters != null)
                    {
                        foreach (var p in parameters)
                        {
                            cmd.Parameters.Add(p);
                        }
                    }

                    // Execute the scalar call.
                    object result = cmd.ExecuteScalar();

                    // If we made it here, the call completed successfully.

                    // Return data to the caller.
                    if (result is DBNull)
                        output = null;
                    else
                        output = result;

                    return 1;
                }
                catch (Exception e)
                {
                    // Something went wrong while attempting to run the query.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                        $"{_classname}:-:{nameof(Execute_Scalar)} - " +
                        "An exception was caught while attempting to run the query.");

                    return -3;
                }
                finally
                {
                    // Clean up the command.
                    this.CloseCommand(cmd);
                    cmd = null;
                }
                // If here, we have result data from the executed query.

            }
            finally
            {
                if(!this._explicit_ConnectionOpen_Called)
                    this.Disconnect();
            }
        }

        /// <summary>
        /// Performs a non-query command.
        /// Accepts a timeout in seconds. 0 for no timeout.
        /// Returns 1 for success. Negatives for error.
        /// </summary>
        /// <param name="querystring"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public int Execute_NonQuery(string querystring, int timeout = 30)
        {
            NpgsqlCommand cmd = null;

            if(this.disposedValue)
            {
                // Already disposed.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Execute_NonQuery)} - " +
                    "Already disposed.");

                return -1;
            }

            try
            {
                // We need a database connection for this query.
                // See if we have an existing one...
                var resconn = this.Connect(false);
                if(resconn != 1)
                {
                    // Failed to open database connection.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Execute_NonQuery)} - " +
                        "Already disposed.");

                    return -1;
                }
                // If here, we have a connection we can use.

                try
                {
                    // Make the query to get result data.
                    cmd = new NpgsqlCommand(querystring, _dbConnection);
                    cmd.CommandTimeout = timeout;

                    int res = cmd.ExecuteNonQuery();
                    return res;
                }
                catch (DbException dbe)
                {
                    // Database Exception occurred.

                    if(dbe.Message.Contains("permission denied"))
                    {
                        // Connected user has inadequate privilege.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:-:{nameof(Execute_NonQuery)} - " +
                            "Connected user has inadequate privilege.");

                        return -10;
                    }
                    else if(dbe.Message.Contains("Timeout Expired"))
                    {
                        // Timeout occurred.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:-:{nameof(Execute_NonQuery)} - " +
                            "Timeout occurred while running database command.");

                        //// Attempting to roll back the command.
                        //try
                        //{
                        //    cmd.CommandText = "IF @@TRANCOUNT>0 ROLLBACK TRAN;";
                        //    cmd.ExecuteNonQuery();
                        //}
                        //catch(Exception g)
                        //{

                        //}

                        return -10;
                    }
                    else
                    {
                        // Non-specific exception.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(dbe,
                            $"{_classname}:-:{nameof(Execute_NonQuery)} - " +
                            "SQLException occurred while running database command.");

                        return -30;
                    }
                }
                catch (Exception e)
                {
                    // Something went wrong while attempting to run the query.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                        $"{_classname}:-:{nameof(Execute_NonQuery)} - " +
                        "Exception occurred while running database command.");

                    return -30;
                }
            }
            finally
            {
                // Clean up the command.
                this.CloseCommand(cmd);
                cmd = null;

                if(!this._explicit_ConnectionOpen_Called)
                    this.Disconnect();
            }
        }

        /// <summary>
        /// Common method for closing a postgres command instance.
        /// </summary>
        /// <param name="cmd"></param>
        private void CloseCommand(NpgsqlCommand cmd)
        {
            try
            {
                cmd?.Cancel();
            }
            catch (Exception) { }
            try
            {
                cmd?.Dispose();
            }
            catch (Exception) { }
        }

        #endregion


        #region Static Field Getters

        /// <summary>
        /// Returns the following:
        ///  1 - Successfully recovered the value.
        ///  0 - Value was null.
        /// -1 - Value was not parseable to the target type.
        /// -2 - Column does not exist.
        /// </summary>
        /// <param name="dr"></param>
        /// <param name="fieldname"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        static public int Recover_FieldValue_from_DBRow(System.Data.DataRow dr, string fieldname, out bool val)
        {
            string tempstr = "";

            try
            {
                // Get the field value.
                tempstr = dr[fieldname] + "";

                // See if the stored value is a null.
                if (tempstr == "")
                {
                    // Value is a null.
                    val = false;
                    return 0;
                }

                // See if the value is true or false.
                if (tempstr == "0")
                    val = false;
                else if (tempstr == "1")
                    val = true;
                else
                    // Not a value we can parse.
                    val = false;
                    return -1;

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Recover_FieldValue_from_DBRow)} - " +
                    "An exception occurred while atttempting to recover a field value from a database row.");

                val = false;
                return -2;
            }
        }
        /// <summary>
        /// Returns the following:
        ///  1 - Successfully recovered the value.
        ///  0 - Value was null.
        /// -1 - Value was not parseable to the target type.
        /// -2 - Column does not exist.
        /// </summary>
        /// <param name="dr"></param>
        /// <param name="fieldname"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        static public int Recover_FieldValue_from_DBRow(System.Data.DataRow dr, string fieldname, out string val)
        {
            string tempstr = "";

            try
            {
                // Get the raw value.
                tempstr = dr[fieldname] + "";

                // See if the stored value is a null.
                if (tempstr == "")
                {
                    // Value is a null.
                    val = "";
                    return 0;
                }

                val = tempstr;

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Recover_FieldValue_from_DBRow)} - " +
                    "An exception occurred while atttempting to recover a field value from a database row.");

                val = "";
                return -2;
            }
        }
        /// <summary>
        /// Returns the following:
        ///  1 - Successfully recovered the value.
        ///  0 - Value was null.
        /// -1 - Value was not parseable to the target type.
        /// -2 - Column does not exist.
        /// </summary>
        /// <param name="dr"></param>
        /// <param name="fieldname"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        static public int Recover_FieldValue_from_DBRow(System.Data.DataRow dr, string fieldname, out int val)
        {
            string tempstr = "";

            try
            {
                // Get the raw value.
                tempstr = dr[fieldname] + "";

                // See if the stored value is a null.
                if (tempstr == "")
                {
                    // Value is a null.
                    val = int.MinValue;
                    return 0;
                }

                // Attempt to convert the value.
                try
                {
                    int sss = Convert.ToInt32(tempstr);

                    val = sss;

                    return 1;
                }
                catch (Exception)
                {
                    val = int.MinValue;
                    return -1;
                }
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Recover_FieldValue_from_DBRow)} - " +
                    "An exception occurred while atttempting to recover a field value from a database row.");

                val = int.MinValue;
                return -2;
            }
        }
        /// <summary>
        /// Returns the following:
        ///  1 - Successfully recovered the value.
        ///  0 - Value was null.
        /// -1 - Value was not parseable to the target type.
        /// -2 - Column does not exist.
        /// </summary>
        /// <param name="dr"></param>
        /// <param name="fieldname"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        static public int Recover_FieldValue_from_DBRow(System.Data.DataRow dr, string fieldname, out float val)
        {
            string tempstr = "";

            try
            {
                // Get the raw value.
                tempstr = dr[fieldname] + "";

                // See if the stored value is a null.
                if (tempstr == "")
                {
                    // Value is a null.
                    val = float.MinValue;
                    return 0;
                }

                // Attempt to convert the value.
                try
                {
                    float sss = Convert.ToSingle(tempstr);

                    val = sss;

                    return 1;
                }
                catch (Exception)
                {
                    val = float.MinValue;
                    return -1;
                }
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Recover_FieldValue_from_DBRow)} - " +
                    "An exception occurred while atttempting to recover a field value from a database row.");

                val = float.MinValue;
                return -2;
            }
        }
        /// <summary>
        /// Returns the following:
        ///  1 - Successfully recovered the value.
        ///  0 - Value was null.
        /// -1 - Value was not parseable to the target type.
        /// -2 - Column does not exist.
        /// </summary>
        /// <param name="dr"></param>
        /// <param name="fieldname"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        static public int Recover_FieldValue_from_DBRow(System.Data.DataRow dr, string fieldname, out DateTime val)
        {
            string tempstr = "";

            try
            {
                // Get the raw value.
                tempstr = dr[fieldname] + "";

                // See if the stored value is a null.
                if (tempstr == "")
                {
                    // Value is a null.
                    val = DateTime.MinValue;
                    return 0;
                }

                // Attempt to convert the value.
                try
                {
                    DateTime sss;

                    if (DateTime.TryParse(tempstr, out sss) == true)
                    {
                        val = sss;

                        return 1;
                    }
                    else
                    {
                        val = DateTime.MinValue;
                        return -1;
                    }
                }
                catch (Exception)
                {
                    val = DateTime.MinValue;
                    return -1;
                }
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Recover_FieldValue_from_DBRow)} - " +
                    "An exception occurred while atttempting to recover a field value from a database row.");

                val = DateTime.MinValue;
                return -2;
            }
        }

        #endregion
    }
}
