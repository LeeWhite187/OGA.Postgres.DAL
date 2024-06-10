using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using NLog.LayoutRenderers;
using NLog.Layouts;
using OGA.Postgres;
using OGA.Postgres.DAL;
using OGA.Postgres.DAL.Model;
using OGA.Postgres.DAL_SP.CreateVerify.Model;
using OGA.Postgres.DAL_SP.Model;
using OpenTelemetry;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Permissions;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace OGA.Postgres.CreateVerify
{
    /// <summary>
    /// Set of methods for managing a Postgres database using a configurable database layout.
    /// Has methods for creating a database from a layout, verifying a database against a layout, creating a layout from a database.
    /// </summary>
    public class DatabaseLayout_Tool
    {
        #region Private Fields

        static private string _classname = nameof(DatabaseLayout_Tool);

        static private volatile int _instancecounter;

        #endregion


        #region Public Properties

        public int InstanceId { get; set; }

        public string Hostname { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        #endregion


        #region ctor / dtor

        public DatabaseLayout_Tool()
        {
            _instancecounter++;
            InstanceId = _instancecounter;
        }

        #endregion


        #region Connectivity Methods

        /// <summary>
        /// Provides a quick ability to test credentials to a PostgreSQL server, without creating a persistent connection.
        /// </summary>
        /// <returns></returns>
        public int TestConnection()
        {
            using(var dal = new Postgres_DAL())
            {
                dal.Hostname = Hostname;
                dal.Database = "postgres";
                dal.Username = Username;
                dal.Password = Password;

                return dal.Test_Connection();
            }
        }

        #endregion


        #region Public Methods

        /// <summary>
        /// Will verify that the given database layout matches the live database and table structure.
        /// Returns 1 if no differences.
        /// Returns 0 with list of differences.
        /// </summary>
        /// <param name="layout"></param>
        /// <param name="ptool"></param>
        /// <returns></returns>
        public (int res, List<VerificationDelta> errs) Verify_Database_Layout(DbLayout_Database layout, Postgres_Tools ptool = null)
        {
            bool localtoolinstance = false;
            var errs = new List<VerificationDelta>();

            try
            {
                // Verify the given layout...
                if(layout == null)
                {
                    // Layout is null.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                        $"Given layout is null. Verification failed.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Layout;
                    err.ObjName = "";
                    err.ParentName = "";
                    err.ErrText = "Null layout";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);

                    return (0, errs);
                }
                // Validate the layout...
                var resval = layout.Validate();
                if(resval.res != 1)
                {
                    // Layout failed validation.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                        $"Given layout failed validation. Verification failed.");

                    // Roll up errors...
                    foreach(var e in resval.errs)
                        errs.Add(e);

                    // The validate call already added errors to the list.
                    //var err = new VerificationDelta();
                    //err.ObjType = eObjType.Layout;
                    //err.ObjName = "";
                    //err.ParentName = "";
                    //err.ErrText = "Layout failed validation";
                    //err.ErrorType = eErrorType.ValidationError;
                    //errs.Add(err);

                    return (0, errs);
                }

                if(ptool == null)
                {
                    localtoolinstance = true;

                    // Create an instance...
                    ptool = new Postgres_Tools();
                    ptool.Hostname = this.Hostname;
                    ptool.Database = "postgres";
                    ptool.Username = this.Username;
                    ptool.Password = this.Password;
                }

                // Check if the database exists...
                var resdbexists = ptool.Is_Database_Present(layout.name);
                if(resdbexists < 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                        $"Failed to access server.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Server;
                    err.ObjName = "";
                    err.ParentName = "";
                    err.ErrText = "Failed to access server";
                    err.ErrorType = eErrorType.DatabaseAccessError;
                    errs.Add(err);

                    return (-1, errs);
                }
                if(resdbexists == 0)
                {
                    // The database is not found.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                        $"Database ({(layout.name)}) not found on server.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Database;
                    err.ObjName = layout.name ?? "";
                    err.ParentName = "";
                    err.ErrText = "Database not found";
                    err.ErrorType = eErrorType.NotFound;
                    errs.Add(err);

                    return (0, errs);
                }
                // The database exists.

                // Verify the database owner is correct...
                if(string.IsNullOrEmpty(layout.owner))
                {
                    // No owner was specified.
                    // Nothing to check.
                }
                else
                {
                    // An owner was specified.
                    // Verify it's correct...

                    var resown = ptool.GetDatabaseOwner(layout.name, out string dbowner);
                    if(resown != 1)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                            $"Failed to get owner of database ({(layout.name)}).");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Server;
                        err.ObjName = layout.name ?? "";
                        err.ParentName = "";
                        err.ErrText = "Failed to retrieve database owner";
                        err.ErrorType = eErrorType.DatabaseAccessError;
                        errs.Add(err);

                        return (-1, errs);
                    }
                    // Retrieved the database owner.

                    if (dbowner != layout.owner)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                            $"Database ({(layout.name)}) owner actual ({(dbowner)}) does not match layout owner ({(layout.owner)}).");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Database;
                        err.ObjName = layout.name ?? "";
                        err.ParentName = "";
                        err.ErrText = "Database Owner Mismatch";
                        err.ErrorType = eErrorType.Different;
                        errs.Add(err);
                    }
                }

                // In order to access database table info, we have to be on a connection with the containing database.
                // Currently, we are connected to the postgres database.
                // So, we will switch to the actual database for the rest of these calls.
                ptool.Dispose();
                ptool = new Postgres_Tools();
                ptool.Hostname = this.Hostname;
                // Use the database in the layout...
                ptool.Database = layout.name;
                ptool.Username = this.Username;
                ptool.Password = this.Password;

                // Verify we can connect...
                if(ptool.TestConnection() != 1)
                {
                    // Failed to connect with database in layout.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                        $"Failed to connect with Database ({(layout.name)}).");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Server;
                    err.ObjName = layout.name ?? "";
                    err.ParentName = "";
                    err.ErrText = "Cannot Connect to Database";
                    err.ErrorType = eErrorType.DatabaseAccessError;
                    errs.Add(err);

                    return (-1, errs);
                }

                // Verify that each table exists...
                foreach(var t in layout.tables)
                {
                    if(t == null)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                            $"Null table in layout.");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Layout;
                        err.ObjName = layout.name ?? "";
                        err.ParentName = "";
                        err.ErrText = "Null table in layout";
                        err.ErrorType = eErrorType.ValidationError;
                        errs.Add(err);

                        continue;
                    }

                    // Verify the current table exists...
                    var restexists = ptool.DoesTableExist(t.name);
                    if(restexists != 1)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                            $"Table ({(t.name ?? "")}) not found in database ({(layout.name)}).");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Table;
                        err.ObjName = t.name ?? "";
                        err.ParentName = layout.name ?? "";
                        err.ErrText = "Table Not Found";
                        err.ErrorType = eErrorType.NotFound;
                        errs.Add(err);
                    }
                    else
                    {
                        // Table was found in live database.

                        // Get table column info...
                        var resci = ptool.Get_ColumnInfo_forTable(t.name, out var livetablecolumnlist);
                        if(resci != 1)
                        {
                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                $"Failed to get column info for table ({(t.name ?? "")}) of database ({(layout.name)}).");

                            var err = new VerificationDelta();
                            err.ObjType = eObjType.Table;
                            err.ObjName = t.name ?? "";
                            err.ParentName = layout.name ?? "";
                            err.ErrText = "Failed to retrieve column info";
                            err.ErrorType = eErrorType.DatabaseAccessError;
                            errs.Add(err);

                            return (-1, errs);
                        }
                        // Retrieved column info for table.

                        if(livetablecolumnlist == null)
                            livetablecolumnlist = new List<ColumnInfo>();

                        // Get primary keys for the table...
                        var respk = ptool.Get_PrimaryKeyConstraints_forTable(t.name, out var pklist);
                        if(respk != 1 || pklist == null)
                        {
                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                $"Failed to get primary key list for table ({(t.name ?? "")}) of database ({(layout.name)}).");

                            var err = new VerificationDelta();
                            err.ObjType = eObjType.Table;
                            err.ObjName = t.name ?? "";
                            err.ParentName = layout.name ?? "";
                            err.ErrText = "Failed to retrieve primary key list";
                            err.ErrorType = eErrorType.DatabaseAccessError;
                            errs.Add(err);

                            return (-1, errs);
                        }
                        // We have column info to verify.
                        // We will verify in both directions, so we identify missing columns, and extra columns.

                        // Verify layout columns are in live table...
                        foreach(var c in livetablecolumnlist)
                        {
                            // Get the column info from our layout...
                            var cli = t.columns.FirstOrDefault(n=>n.name == c.name);
                            if(cli == null)
                            {
                                // The live table has a column name that is NOT in the layout.

                                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                    $"Live table ({(t.name ?? "")}) has column ({(c.name)}) that is not in table ({(t.name)}).");

                                var err = new VerificationDelta();
                                err.ObjType = eObjType.Column;
                                err.ObjName = c.name ?? "";
                                err.ParentName = t.name ?? "";
                                err.ErrText = "Table column missing from layout";
                                err.ErrorType = eErrorType.Extra;
                                errs.Add(err);

                                continue;
                            }
                            // Live table column name is in layout.

                            // Verify its type...
                            var resctypematch = this.DoesColumnTypeMatchLayoutType(c.dataType, cli.dataType);
                            if(!resctypematch)
                            {
                                // The live table column datatype is not a match to the layout column datatype.

                                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                    $"Live table ({(t.name ?? "")}) column ({(c.name)}) datatype ({(c.dataType)}) doesn't match layout column datatype ({(cli.dataType.ToString())}).");

                                var err = new VerificationDelta();
                                err.ObjType = eObjType.Column;
                                err.ObjName = c.name ?? "";
                                err.ParentName = t.name ?? "";
                                err.ErrText = "Column datatype different from layout";
                                err.ErrorType = eErrorType.Different;
                                errs.Add(err);
                            }
                            else
                            {
                                // Datatypes are a match.

                                // Verify length if a varchar...
                                if(cli.dataType == DAL_SP.CreateVerify.Model.eColDataTypes.varchar)
                                {
                                    // Verify the varchar length is a match...
                                    if(c.maxlength != cli.maxlength)
                                    {
                                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                            $"Live table ({(t.name ?? "")}) varchar column ({(c.name)}) has different max length than layout.");

                                        var err = new VerificationDelta();
                                        err.ObjType = eObjType.Column;
                                        err.ObjName = c.name ?? "";
                                        err.ParentName = t.name ?? "";
                                        err.ErrText = "Varchar column has different max length from layout";
                                        err.ErrorType = eErrorType.Different;
                                        errs.Add(err);
                                    }
                                }
                            }


                            // Verify its nullable state...
                            if(c.isNullable != cli.isNullable)
                            {
                                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                    $"Live table ({(t.name ?? "")}) column ({(c.name)}) has different isnullable setting from layout.");

                                var err = new VerificationDelta();
                                err.ObjType = eObjType.Column;
                                err.ObjName = c.name ?? "";
                                err.ParentName = t.name ?? "";
                                err.ErrText = "Column isnullable different from layout";
                                err.ErrorType = eErrorType.Different;
                                errs.Add(err);
                            }

                            // Check if it's a primary key...
                            if(pklist.Exists(n=>n.key_column == c.name))
                            {
                                // Current live table column is a primary key.

                                // Check that our layout indicates that...
                                if(cli.dataType == DAL_SP.CreateVerify.Model.eColDataTypes.pk_integer ||
                                    cli.dataType == DAL_SP.CreateVerify.Model.eColDataTypes.pk_bigint ||
                                    cli.dataType == DAL_SP.CreateVerify.Model.eColDataTypes.pk_uuid)
                                {
                                    // The column in the layout is a primary key.
                                    // This matches the column in live table.
                                }
                                else
                                {
                                    // Layout indicates column is NOT a primary key.

                                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                        $"Live table ({(t.name ?? "")}) column ({(c.name)}) is a primary key, but layout shows it is NOT one.");

                                    var err = new VerificationDelta();
                                    err.ObjType = eObjType.Column;
                                    err.ObjName = c.name ?? "";
                                    err.ParentName = t.name ?? "";
                                    err.ErrText = "Live table column should NOT be primary key";
                                    err.ErrorType = eErrorType.Different;
                                    errs.Add(err);
                                }
                            }
                            else
                            {
                                // Not a primary key.
                                // Check if it should be...

                                // See if the current live table column is a primary key in the layout...
                                if(cli.dataType == DAL_SP.CreateVerify.Model.eColDataTypes.pk_integer ||
                                    cli.dataType == DAL_SP.CreateVerify.Model.eColDataTypes.pk_bigint ||
                                    cli.dataType == DAL_SP.CreateVerify.Model.eColDataTypes.pk_uuid)
                                {
                                    // The column in the layout is a primary key.
                                    // But, the live table column is not one.

                                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                        $"Live table ({(t.name ?? "")}) column ({(c.name)}) is NOT a primary key, but layout shows it as one.");

                                    var err = new VerificationDelta();
                                    err.ObjType = eObjType.Column;
                                    err.ObjName = c.name ?? "";
                                    err.ParentName = t.name ?? "";
                                    err.ErrText = "Live table column should be primary key";
                                    err.ErrorType = eErrorType.Different;
                                    errs.Add(err);
                                }
                                else
                                {
                                    // Layout and live table both indicate not a primary key.
                                }
                            }
                        }
                        // We've iterated all live table columns, and verified ones that are in the layout.
                        // As well, we've marked live table columns, that are not in the layout, as errors.
                        // What's left to reconcile is columns in the layout that are NOT columns in the live table.

                        // Verify layout columns are in the live table...
                        foreach(var c in t.columns)
                        {
                            if(c == null)
                            {
                                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                    $"Null column in table ({(t.name)}) of layout.");

                                var err = new VerificationDelta();
                                err.ObjType = eObjType.Column;
                                err.ObjName = "";
                                err.ParentName = t.name ?? "";
                                err.ErrText = "Null column in layout";
                                err.ErrorType = eErrorType.DatabaseAccessError;
                                errs.Add(err);

                                continue;
                            }

                            // Get the live table column...
                            var ltc = livetablecolumnlist.FirstOrDefault(n=>n.name == c.name);
                            if(ltc == null)
                            {
                                // Layout has a column name that is NOT in the live table.

                                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Verify_Database_Layout)} - " +
                                    $"Layout has column ({(c.name)}) that is not in live table ({(t.name)}).");

                                var err = new VerificationDelta();
                                err.ObjType = eObjType.Column;
                                err.ObjName = c.name ?? "";
                                err.ParentName = t.name ?? "";
                                err.ErrText = "Table column missing from live table";
                                err.ErrorType = eErrorType.NotFound;
                                errs.Add(err);
                            }
                            // Live table column name is in layout.
                            // We've already checked properties of the live table columns against their matches in the layout.
                            // So, we can skip to the next one.
                        }
                    }
                }

                if(errs.Count != 0)
                    return (0, errs);
                else
                    return (1, errs);
            }
            finally
            {
                if(localtoolinstance)
                {
                    try
                    {
                        ptool?.Dispose();
                    }
                    catch (Exception) { }
                    ptool = null;
                }
            }
        }

        /// <summary>
        /// Will create a layout graph for a given database.
        /// This can be used for creating the database on another host, and to verify its layout.
        /// Returns 1 if successful, 0 if the database is not found, negatives for errors.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public (int res, DbLayout_Database layout) CreateLayout_fromDatabase(string databaseName)
        {
            Postgres_Tools ptool = null;

            try
            {
                if(string.IsNullOrEmpty(databaseName))
                {
                    // Database name is blank.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                        $"Given database name is empty. Cannot create layout.");

                    return (-1, null);
                }

                if(ptool == null)
                {
                    // Create an instance...
                    ptool = new Postgres_Tools();
                    ptool.Hostname = this.Hostname;
                    ptool.Database = "postgres";
                    ptool.Username = this.Username;
                    ptool.Password = this.Password;
                }

                // Check if the database exists...
                var resdbexists = ptool.Is_Database_Present(databaseName);
                if(resdbexists < 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                        $"Failed to access server.");

                    return (-2, null);
                }
                if(resdbexists == 0)
                {
                    // The database was NOT found.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                        $"Database ({(databaseName)}) doesn't exixt. Cannot extract layout.");

                    return (-3, null);
                }
                // The database exists.
                // We will create a layout from it.

                // Start a layout...
                var layout = new DbLayout_Database();
                layout.name = databaseName;

                // Get the database owner...
                var resown = ptool.GetDatabaseOwner(databaseName, out var dbowner);
                if(resown != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                        $"Failed to get database owner.");

                    return (-4, null);
                }
                layout.owner = dbowner;

                // Change connection to the database, so we can query its structure...
                {
                    ptool.Dispose();
                    ptool = new Postgres_Tools();
                    ptool.Hostname = this.Hostname;
                    ptool.Database = layout.name;
                    ptool.Username = this.Username;
                    ptool.Password = this.Password;
                }

                // Get a list of tables in the database...
                var res2= ptool.Get_TableList_forDatabase(databaseName, out var tbllist);
                if(res2 != 1 || tbllist == null)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                        $"Failed to get table list for database ({(databaseName)}).");

                    return (-5, null);
                }

                // Sort them alphabetically...
                tbllist.Sort((a, b) => a.CompareTo(b));

                // Iterate each table, and build a layout for it...
                int ordinalindex = 0;
                foreach(var t in tbllist)
                {
                    // Start a table entry...
                    var tlayout = new DbLayout_Table();

                    tlayout.name = t;
                    ordinalindex++;
                    tlayout.ordinal = ordinalindex;

                    // Get column data for the current table...
                    var rescol = ptool.Get_ColumnInfo_forTable(t, out var collist);
                    if(rescol != 1 || collist == null)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                            $"Failed to get column info list for table ({(t)}) of database ({(databaseName)}).");

                        return (-6, null);
                    }

                    // We need to know what table columns are primary keys...
                    var respk = ptool.Get_PrimaryKeyConstraints_forTable(t, out var pklist);
                    if(respk != 1 || pklist == null)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                            $"Failed to get primary key constraints for table ({(t)}) of database ({(databaseName)}).");

                        return (-7, null);
                    }

                    // Create column layout entries for each column in the table...
                    foreach (var cdef in collist)
                    {
                        // Start a column entry..
                        var clayout = new DbLayout_Column();
                        clayout.name = cdef.name;
                        clayout.ordinal = cdef.ordinal;
                        clayout.isNullable = cdef.isNullable;
                        clayout.isIdentity = cdef.isIdentity;

                        // Parse the column's datatype...
                        var rescoltype = DatabaseLayout_Tool.Parse_ColDataType(cdef.dataType);
                        if(rescoltype.res != 1)
                        {
                            // Failed to resolve column type.

                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                                $"Failed to resolve column type ({(cdef.dataType)}) for column ({(cdef.name)}) of table ({(t)}) of database ({(databaseName)}).");

                            return (-8, null);
                        }
                        // We have the column type.

                        // We need to promote it if it's a primary key column...
                        var pkc = pklist.FirstOrDefault(n => n.key_column == cdef.name);
                        if(pkc != null)
                        {
                            // The current column is a primary key column.
                            // We will promote the column type.

                            var respromote = PromoteColType_toPrimaryKeyType(rescoltype.dtype);
                            if(respromote.res != 1)
                            {
                                // Failed to identify primary key column type for given datatype.

                                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                                    $"Failed to resolve primary key column type for column datatype ({(cdef.dataType)}) " +
                                    $"of column ({(cdef.name)}) of table ({(t)}) of database ({(databaseName)}).");

                                return (-9, null);
                            }
                            // If here, we have a promoted primary key column type.

                            clayout.dataType = respromote.dtype;
                        }
                        else
                        {
                            // The column is NOT a primary key.
                            // We will accept its column datatype.
                            clayout.dataType = rescoltype.dtype;
                        }

                        // Default max length to null, unless we set it below...
                        clayout.maxlength = null;

                        // If the column is a varchar, we need its max length..
                        if(clayout.dataType == eColDataTypes.varchar)
                            clayout.maxlength = cdef.maxlength;

                        // If the column is a text type, we will set length to zero...
                        if(clayout.dataType == eColDataTypes.text)
                            clayout.maxlength = 0;

                        // Add the column to our running list...
                        tlayout.columns.Add(clayout);
                    }
                    // We've iterated the columns of the current table.
                    // We can add the current table to the layout.

                    layout.tables.Add(tlayout);
                }
                // We have created the layout for the given database.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                    $"Layout created for database ({(databaseName)}).");

                return (1, layout);
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(CreateLayout_fromDatabase)} - " +
                    $"Exception occurred while creating layout from database.");

                return (-10, null);
            }
            finally
            {
                try
                {
                    ptool?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Creates a database from a given layout.
        /// Will return error if the database exists.
        /// </summary>
        /// <param name="layout"></param>
        /// <returns></returns>
        public (int res, List<VerificationDelta> errs) Create_Database_fromLayout(DbLayout_Database layout)
        {
            Postgres_Tools ptool = null;
            var errs = new List<VerificationDelta>();

            try
            {
                if(layout == null)
                {
                    // Layout is null.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                        $"Given layout is null. Cannot create database.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Layout;
                    err.ObjName = "layout";
                    err.ParentName = "";
                    err.ErrText = "Layout is NULL";
                    err.ErrorType = eErrorType.NotFound;
                    errs.Add(err);

                    return (-1, errs);
                }

                // Validate the layout...
                var resval = layout.Validate();
                if(resval.res != 1)
                {
                    // Layout validation failed.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                        $"Layout validation failed. Cannot create database.");

                    foreach (var er in resval.errs)
                        errs.Add(er);

                    // The validate call already added errors to the list.
                    //var err = new VerificationDelta();
                    //err.ObjType = eObjType.Layout;
                    //err.ObjName = "layout";
                    //err.ParentName = "";
                    //err.ErrText = "Layout failed validation";
                    //err.ErrorType = eErrorType.NotFound;
                    //errs.Add(err);

                    return (-1, errs);
                }
                // Layout is good to go.

                if(ptool == null)
                {
                    // Create an instance...
                    ptool = new Postgres_Tools();
                    ptool.Hostname = this.Hostname;
                    ptool.Database = "postgres";
                    ptool.Username = this.Username;
                    ptool.Password = this.Password;
                }

                // Check if the database exists...
                var resdbexists = ptool.Is_Database_Present(layout.name);
                if(resdbexists < 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                        $"Failed to access server.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Server;
                    err.ObjName = "Server";
                    err.ParentName = "";
                    err.ErrText = "Failed to access server";
                    err.ErrorType = eErrorType.DatabaseAccessError;
                    errs.Add(err);

                    return (-2, errs);
                }
                if(resdbexists == 1)
                {
                    // The database was found.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                        $"Database ({(layout.name)}) already exists. Cannot create.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Database;
                    err.ObjName = layout.name;
                    err.ParentName = "";
                    err.ErrText = "Database Already Exists";
                    err.ErrorType = eErrorType.Extra;
                    errs.Add(err);

                    return (-3, errs);
                }
                // The database does not yet exist.
                // We will attempt to create it.

                // Before we continue, we need to check that the credentials are allowed to create a database...
                {
                    var rescheck1 = ptool.IsSuperUser(this.Username);
                    if(rescheck1 < 0)
                    {
                        // Failed to check user role.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                            $"Failed to check if user ({(this.Username)}) is SuperUser. Cannot create.");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Server;
                        err.ObjName = "";
                        err.ParentName = "";
                        err.ErrText = "Cannot check privilege";
                        err.ErrorType = eErrorType.DatabaseAccessError;
                        errs.Add(err);

                        return (-4, errs);
                    }
                    if(rescheck1 == 0)
                    {
                        // User is not a superuser.
                        // They could still have the CREATEDB role.

                        var rescreatedbrole = ptool.HasDBCreate(this.Username);
                        if(rescreatedbrole != 1)
                        {
                            // User doesn't have CREATEDB or SUPERUSER roles.

                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                                $"Given user ({(this.Username)}) cannot create database. Cannot continue.");

                            var err = new VerificationDelta();
                            err.ObjType = eObjType.Server;
                            err.ObjName = this.Username;
                            err.ParentName = "";
                            err.ErrText = "User Missing CREATEDB Role";
                            err.ErrorType = eErrorType.DatabaseAccessError;
                            errs.Add(err);

                            return (-5, errs);
                        }
                        // User has the CREATEDB role.
                    }
                    // User has CREATEDB or SUPERUSER role.
                    // He can create the database.
                }

                // Create the database...
                var rescreate = ptool.Create_Database(layout.name);
                if(rescreate != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                        $"Failed to create database ({(layout.name)}). Cannot create.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Database;
                    err.ObjName = layout.name;
                    err.ParentName = "";
                    err.ErrText = "Failed to create database";
                    err.ErrorType = eErrorType.DatabaseAccessError;
                    errs.Add(err);

                    return (-6, errs);
                }

                // Verify the database, now exists...
                if(ptool.Is_Database_Present(layout.name) != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                        $"Failed to create database.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Server;
                    err.ObjName = "Server";
                    err.ParentName = "";
                    err.ErrText = "Failed to create database";
                    err.ErrorType = eErrorType.DatabaseAccessError;
                    errs.Add(err);

                    return (-7, errs);
                }

                // Set the database owner if needed...
                if(!string.IsNullOrEmpty(layout.owner))
                {
                    // The layout includes a database owner.
                    // We will set the database owner.

                    // Set the owner for the database we just created...
                    var resowner = ptool.ChangeDatabaseOwner(layout.name, layout.owner);
                    if(resowner != 1)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                            $"Failed to set owner for database ({(layout.name)}). Cannot create.");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Database;
                        err.ObjName = layout.name;
                        err.ParentName = "";
                        err.ErrText = "Failed to create database";
                        err.ErrorType = eErrorType.DatabaseAccessError;
                        errs.Add(err);

                        return (-8, errs);
                    }
                }

                // Change connection to the new database, so we can add tables to it...
                {
                    ptool.Dispose();
                    ptool = new Postgres_Tools();
                    ptool.Hostname = this.Hostname;
                    ptool.Database = layout.name;
                    ptool.Username = this.Username;
                    ptool.Password = this.Password;
                }

                
                // Create each table for it...
                foreach(var t in layout.tables)
                {
                    if (t == null)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                            $"Null table in layout for database ({(layout.name)}). Cannot create.");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Table;
                        err.ObjName = layout.name;
                        err.ParentName = "";
                        err.ErrText = "Null table";
                        err.ErrorType = eErrorType.ValidationError;
                        errs.Add(err);

                        return (-9, errs);
                    }

                    var tch = new TableDefinition(t.name, layout.owner);
                    tch.tablespace = "pg_default";

                    // Iterate columns and add them to the definition...
                    foreach(var c in t.columns)
                    {
                        if (c == null)
                        {
                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                                $"Null column in table defintion for database ({(layout.name)}). Cannot create.");

                            var err = new VerificationDelta();
                            err.ObjType = eObjType.Table;
                            err.ObjName = t.name;
                            err.ParentName = layout.name;
                            err.ErrText = "Null table column";
                            err.ErrorType = eErrorType.ValidationError;
                            errs.Add(err);

                            return (-10, errs);
                        }

                        // Add the current column to the table defintion...
                        var resaddcol = this.AddColumntoTableDef(tch, c);
                        if(resaddcol != 1)
                        {
                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                                $"Failed to add column to table defintion for database ({(layout.name)}). Cannot create.");

                            var err = new VerificationDelta();
                            err.ObjType = eObjType.Column;
                            err.ObjName = c.name;
                            err.ParentName = t.name;
                            err.ErrText = "Failed to add column to table def";
                            err.ErrorType = eErrorType.ValidationError;
                            errs.Add(err);

                            return (-11, errs);
                        }
                    }
                    // We've added all columns to the table definition.

                    // Generate the table creation script...
                    var createsql = tch.CreateSQLCmd();
                    if(string.IsNullOrEmpty(createsql))
                    {
                        // Failed to create sql for table.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                            $"Failed to generate table creation script for table ({(t.name)}) database ({(layout.name)}). Cannot create.");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Table;
                        err.ObjName = t.name;
                        err.ParentName = layout.name;
                        err.ErrText = "Failed to generate table def";
                        err.ErrorType = eErrorType.ValidationError;
                        errs.Add(err);

                        return (-12, errs);
                    }
                    // We have the table creation script.

                    // Create the table...
                    var rescreatetable = ptool.Create_Table(tch);
                    if(rescreatetable != 1)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                            $"Failed to create table ({(t.name)}) for database ({(layout.name)}). Cannot continue.");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Table;
                        err.ObjName = t.name;
                        err.ParentName = layout.name;
                        err.ErrText = "Failed to create table";
                        err.ErrorType = eErrorType.DatabaseAccessError;
                        errs.Add(err);

                        return (-13, errs);
                    }
                    // We've created the current table.
                }
                // If here, we've used the given layout to create the database and all tables.
                // We should be done.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                    $"Database ({(layout.name)}) successfully created from given layout.");

                return (1, errs);
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database_fromLayout)} - " +
                    $"Exception occurred while creating layout from database.");

                var err = new VerificationDelta();
                err.ObjType = eObjType.Layout;
                err.ObjName = "";
                err.ParentName = "";
                err.ErrText = "Exception during database creation";
                err.ErrorType = eErrorType.DatabaseAccessError;
                errs.Add(err);

                return (-20, errs);
            }
            finally
            {
                try
                {
                    ptool?.Dispose();
                }
                catch (Exception) { }
            }
        }

        #endregion


        #region DataType Conversionss

        /// <summary>
        /// Will promote a given datatype to the corresponding primary key column type, if possible.
        /// Returns 1 for success, negative if no primary key column type is known for the given type.
        /// </summary>
        /// <param name="dtype"></param>
        /// <returns></returns>
        static public (int res, eColDataTypes dtype) PromoteColType_toPrimaryKeyType(eColDataTypes dtype)
        {
            if (dtype == eColDataTypes.bigint)
                return (1, eColDataTypes.pk_bigint);
            else if (dtype == eColDataTypes.integer)
                return (1, eColDataTypes.pk_integer);
            else if (dtype == eColDataTypes.uuid)
                return (1, eColDataTypes.pk_uuid);
            else
                return (-1, eColDataTypes.notset);
        }

        /// <summary>
        /// Parses a given PostGreSQL column datatype (returned from a database information schema query).
        /// Returns 1 if converted, negatives for unknown type.
        /// </summary>
        /// <param name="dataType"></param>
        /// <returns></returns>
        static public (int res, eColDataTypes dtype) Parse_ColDataType(string dataType)
        {
            if(string.IsNullOrEmpty(dataType))
                return (-1, eColDataTypes.notset);

            if (dataType == "bigint")
                return (1, eColDataTypes.bigint);
            else if (dataType == "double precision")
                return (1, eColDataTypes.double_precision);
            else if (dataType == "integer")
                return (1, eColDataTypes.integer);
            else if (dataType == "numeric")
                return (1, eColDataTypes.numeric);
            /* Exclude primary key column types, here, since we have a separate method for promoting a type to a primary key.
            else if (dataType == "bigint")
                return (1, eColDataTypes.pk_bigint);
            else if (dataType == "integer")
                return (1, eColDataTypes.pk_integer);
            else if (dataType == "uuid")
                return (1, eColDataTypes.pk_uuid);
            */
            else if (dataType == "real")
                return (1, eColDataTypes.real);
            else if (dataType == "text")
                return (1, eColDataTypes.text);
            else if (dataType == "timestamp without time zone")
                return (1, eColDataTypes.timestamp);
            else if (dataType == "timestamp with time zone")
                return (1, eColDataTypes.timestampUTC);
            else if (dataType == "uuid")
                return (1, eColDataTypes.uuid);
            else if (dataType == "character varying")
                return (1, eColDataTypes.varchar);
            else
                return (-1, eColDataTypes.notset);
        }

        #endregion


        #region Private Methods

        /// <summary>
        /// Creates a column entry in the given table definition for the given column layout entry.
        /// </summary>
        /// <param name="tch"></param>
        /// <param name="collayoutentry"></param>
        /// <returns></returns>
        private int AddColumntoTableDef(TableDefinition tch, DbLayout_Column collayoutentry)
        {
            int res = -1;

            if(tch == null)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(AddColumntoTableDef)} - " +
                    $"Table definition is null.");

                return -11;
            }
            if(collayoutentry == null)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(AddColumntoTableDef)} - " +
                    $"Column layout is null.");

                return -11;
            }

            // Call the appropriate column add handler based on the layout type...
            if(collayoutentry.dataType == eColDataTypes.bigint)
            {
                res = tch.Add_Numeric_Column(collayoutentry.name, eNumericColTypes.bigint, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.double_precision)
            {
                res = tch.Add_Numeric_Column(collayoutentry.name, eNumericColTypes.double_precision, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.integer)
            {
                res = tch.Add_Numeric_Column(collayoutentry.name, eNumericColTypes.integer, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.numeric)
            {
                res = tch.Add_Numeric_Column(collayoutentry.name, eNumericColTypes.numeric, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.pk_bigint)
            {
                res = tch.Add_Pk_Column(collayoutentry.name, ePkColTypes.bigint);
            }
            else if(collayoutentry.dataType == eColDataTypes.pk_integer)
            {
                res = tch.Add_Pk_Column(collayoutentry.name, ePkColTypes.integer);
            }
            else if(collayoutentry.dataType == eColDataTypes.pk_uuid)
            {
                res = tch.Add_Pk_Column(collayoutentry.name, ePkColTypes.uuid);
            }
            else if(collayoutentry.dataType == eColDataTypes.real)
            {
                res = tch.Add_Numeric_Column(collayoutentry.name, eNumericColTypes.real, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.text)
            {
                res = tch.Add_String_Column(collayoutentry.name, 0, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.timestamp)
            {
                res = tch.Add_DateTime_Column(collayoutentry.name, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.timestampUTC)
            {
                res = tch.Add_UTCDateTime_Column(collayoutentry.name, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.uuid)
            {
                res = tch.Add_Guid_Column(collayoutentry.name, collayoutentry.isNullable);
            }
            else if(collayoutentry.dataType == eColDataTypes.varchar)
            {
                if (collayoutentry.maxlength != null && collayoutentry.maxlength.HasValue)
                {
                    res = tch.Add_String_Column(collayoutentry.name, collayoutentry.maxlength.Value, collayoutentry.isNullable);
                }
                else
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(AddColumntoTableDef)} - " +
                        $"Column ({(collayoutentry.name)}) is varchar, but maxlength is not defined.");

                    res = -12;
                }
            }
            else if(collayoutentry.dataType == eColDataTypes.notset)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(AddColumntoTableDef)} - " +
                    $"Column ({(collayoutentry.name)}) is varchar, but maxlength is not defined.");

                res = -13;
            }

            return res;
        }

        /// <summary>
        /// Checks that the given live table datatype matches with the layout datatype.
        /// </summary>
        /// <param name="livetable_dataType"></param>
        /// <param name="layout_datatype"></param>
        /// <returns></returns>
        private bool DoesColumnTypeMatchLayoutType(string livetable_dataType, eColDataTypes layout_datatype)
        {
            if(layout_datatype == eColDataTypes.pk_uuid && livetable_dataType == "uuid")
                return true;
            if(layout_datatype == eColDataTypes.pk_integer && livetable_dataType == "integer")
                return true;
            if(layout_datatype == eColDataTypes.pk_bigint && livetable_dataType == "bigint")
                return true;

            if(layout_datatype == eColDataTypes.timestampUTC && livetable_dataType == "timestamp with time zone")
                return true;
            if(layout_datatype == eColDataTypes.timestamp && livetable_dataType == "timestamp without time zone")
                return true;

            if(layout_datatype == eColDataTypes.integer && livetable_dataType == "integer")
                return true;
            if(layout_datatype == eColDataTypes.bigint && livetable_dataType == "bigint")
                return true;
            if(layout_datatype == eColDataTypes.real && livetable_dataType == "real")
                return true;
            if(layout_datatype == eColDataTypes.double_precision && livetable_dataType == "double precision")
                return true;
            if(layout_datatype == eColDataTypes.numeric && livetable_dataType == "numeric")
                return true;

            if(layout_datatype == eColDataTypes.varchar && livetable_dataType == "character varying")
                return true;
            if(layout_datatype == eColDataTypes.text && livetable_dataType == "text")
                return true;

            if(layout_datatype == eColDataTypes.uuid && livetable_dataType == "uuid")
                return true;

            // Not a match...
            return false;
        }

        /// <summary>
        /// Compares two given layouts.
        /// Returns 1 if same.
        /// </summary>
        /// <param name="layout1"></param>
        /// <param name="layout2"></param>
        /// <param name="tableordinalsmustmatch"></param>
        /// <param name="ordinalmustmatch"></param>
        /// <returns></returns>
        static public int CompareLayouts(DbLayout_Database layout1, DbLayout_Database layout2, bool tableordinalsmustmatch = false, bool ordinalmustmatch = false)
        {
            if (layout1 == null || layout2 == null)
                return -1;

            if(layout1.name != layout2.name)
                return -1;

            // If either database owner is null or postgres, the other layout owner can be null or postgress.
            // Otherwise, they must match exactly.
            if(layout1.owner != layout2.owner)
            {
                if((string.IsNullOrEmpty(layout1.owner) || layout1.owner == "postgres") &&
                    (string.IsNullOrEmpty(layout2.owner) || layout2.owner == "postgres"))
                {
                    // Layout1 is owned by postgres.
                    // Layout2 is owned by postgres.
                    // The two layouts have the postgres owner.
                }
                else
                {
                    // Only one has a postgres owner, or neither do.
                    // They are different.
                    return -1;
                }
            }

            // Iterate tables...
            // Look for columns not in layout2, and columns that are different
            foreach(var t in layout1.tables)
            {
                var l2t = layout2.tables.FirstOrDefault(n=>n.name == t.name);
                if(l2t == null)
                {
                    // Table missing from layout 2.
                    return -1;
                }

                // Iterate columns in each table.
                var restblcmp = CompareTableLayouts(t, l2t, tableordinalsmustmatch, ordinalmustmatch);
                if(restblcmp != 1)
                {
                    // Tables are different.
                    return -1;
                }
            }

            // Iterate tables from the other...
            // Look for columns not in layout1.
            foreach(var t in layout2.tables)
            {
                var l1t = layout1.tables.FirstOrDefault(n=>n.name == t.name);
                if(l1t == null)
                {
                    // Table missing from layout 1.
                    return -1;
                }
                // We have a match.
                // But, we already compared tables that exist in both, in the previous loop.
                // So, we can skip along.
            }
            // If here, we found no differences.

            return 1;
        }

        /// <summary>
        /// Compares two table layouts, and returns 1 if same.
        /// </summary>
        /// <param name="t1"></param>
        /// <param name="t2"></param>
        /// <param name="tableordinalsmustmatch"></param>
        /// <param name="ordinalmustmatch"></param>
        /// <returns></returns>
        static public int CompareTableLayouts(DbLayout_Table t1, DbLayout_Table t2, bool tableordinalsmustmatch = false, bool ordinalmustmatch = false)
        {
            if (t1 == null || t2 == null)
                return -1;

            if (t1.name != t2.name)
                return -1;

            // Check if ordinals must match...
            if(tableordinalsmustmatch)
            {
                if(t1.ordinal != t2.ordinal)
                    return -1;
            }

            // Iterate columns for matches, and ones missing from t2...
            foreach(var c in t1.columns)
            {
                var c2 = t2.columns.FirstOrDefault(n => n.name == c.name);
                if (c2 == null)
                    return -1;

                // Compare the two columns...
                var rescolcmp = CompareColumnLayouts(c, c2, ordinalmustmatch);
                if(rescolcmp != 1)
                    return -1;
            }

            // Now, look for missing columns in t1, that are in t2...
            foreach(var c in t2.columns)
            {
                var c1 = t1.columns.FirstOrDefault(n => n.name == c.name);
                if (c1 == null)
                    return -1;
            }

            return 1;
        }

        /// <summary>
        /// Compares two column layouts, and returns 1 if same.
        /// </summary>
        /// <param name="c1"></param>
        /// <param name="c2"></param>
        /// <param name="ordinalmustmatch"></param>
        /// <returns></returns>
        static public int CompareColumnLayouts(DbLayout_Column c1, DbLayout_Column c2, bool ordinalmustmatch = false)
        {
            if (c1 == null || c2 == null) return -1;

            if (c1.name != c2.name) return -1;

            if(c1.dataType != c2.dataType) return -1;

            // Check max length if varchar...
            if(c1.dataType == eColDataTypes.varchar)
            {
                if(c1.maxlength != c2.maxlength) return -1;
            }

            if(c1.isIdentity != c2.isIdentity) return -1;

            if(c1.isNullable != c2.isNullable) return -1;

            // Check if we are to enforce ordinal matches...
            if(ordinalmustmatch)
            {
                if(c1.ordinal != c2.ordinal) return -1;
            }

            return 1;
        }

        #endregion
    }
}
