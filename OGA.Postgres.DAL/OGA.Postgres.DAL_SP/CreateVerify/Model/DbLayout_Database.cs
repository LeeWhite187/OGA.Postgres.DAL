using OGA.Postgres.DAL_SP.CreateVerify.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL_SP.Model
{
    /// <summary>
    /// Holds the layout and schema for a database.
    /// Contains a list of tables, and associated columns, and other properties.
    /// </summary>
    public class DbLayout_Database
    {
        /// <summary>
        /// Holds the database name.
        /// </summary>
        public string name { get; set; }

        /// <summary>
        /// If specified, the creation logic will set the database owner to this user.
        /// If blank, the database owner will be the user of the database connection.
        /// </summary>
        public string owner { get; set; }

        /// <summary>
        /// List of tables in the database.
        /// </summary>
        public List<DbLayout_Table> tables { get; set; }


        /// <summary>
        /// Default Constructor
        /// </summary>
        public DbLayout_Database()
        {
            name = "";
            owner = "";
            tables = new List<DbLayout_Table>();
        }

        /// <summary>
        /// Call this method to validate the database layout, before a create or verification is performed.
        /// Returns 1 if passed validation.
        /// Negatives and errors for failures.
        /// </summary>
        /// <returns></returns>
        public (int res, List<VerificationDelta> errs) Validate()
        {
            var errs = new List<VerificationDelta>();

            // Verify the database has a valid name...
            if(!Postgres_Tools.DatabaseNameIsValid(name))
            {
                // The database name is not valid.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(DbLayout_Database)}:-:{nameof(Validate)} - " +
                    $"Invalid database name ({(this.name ?? "")}).");

                var err = new VerificationDelta();
                err.ObjType = eObjType.Database;
                err.ObjName = name ?? "";
                err.ParentName = "";
                err.ErrText = "Invalid database name";
                err.ErrorType = eErrorType.ValidationError;
                errs.Add(err);
            }

            // Verify the database owner is valid, if set...
            if(!string.IsNullOrEmpty(this.owner))
            {
                if(!Postgres_Tools.UserNameIsValid(this.owner))
                {
                    // The database owner is not valid.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Database)}:-:{nameof(Validate)} - " +
                        $"Invalid database owner ({(this.owner ?? "")}).");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Layout;
                    err.ObjName = name ?? "";
                    err.ParentName = "";
                    err.ErrText = "Invalid database owner";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }
            }

            // Verify table properties...
            foreach(var t in tables)
            {
                if(t == null)
                {
                    // Table is null.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Database)}:-:{nameof(Validate)} - " +
                        $"Null table encountered.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Table;
                    err.ObjName = name ?? "";
                    err.ParentName = "";
                    err.ErrText = "Null table";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                    continue;
                }
                /* Skipping table name validation, here, as it's done by the table's own validate method.
                if(!Postgres_Tools.TableNameIsValid(t.name))
                {
                    // Table name is not valid.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Database)}:-:{nameof(Validate)} - " +
                        $"Invalid table name ({(t.name ?? "")}) in database ({(name ?? "")}).");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Table;
                    err.ObjName = t.name ?? "";
                    err.ParentName = this.name ?? "";
                    err.ErrText = "Invalid table name";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }
                */
                // Verify table ordinal is unique...
                if(this.tables.Exists(n => n.ordinal == t.ordinal && n.name != t.name))
                {
                    // Table has duplicate ordinal.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Database)}:-:{nameof(Validate)} - " +
                        $"Duplicate ordinal for table ({(t.name ?? "")}) in database ({(name ?? "")}).");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Table;
                    err.ObjName = t.name ?? "";
                    err.ParentName = this.name ?? "";
                    err.ErrText = "Duplicate Table ordinal";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }
            }

            // Verify each table layout...
            foreach(var t in tables)
            {
                if(t == null)
                {
                    // Table is null.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Database)}:-:{nameof(Validate)} - " +
                        $"Null table encountered.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Table;
                    err.ObjName = this.name ?? "";
                    err.ParentName = "";
                    err.ErrText = "Null table";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                    continue;
                }

                // Validate the table's columns, primary key, etc...
                var resval = t.Validate(this.name);
                if(resval.res != 1)
                {
                    // Layout failed validation.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Database)}:-:{nameof(Validate)} - " +
                        $"Table ({(t.name ?? "")}) of database ({(this.name ?? "")}) failed validation.");

                    // The specific error has already been claimed.
                    //var err = new VerificationDelta();
                    //err.ObjType = eObjType.Table;
                    //err.ObjName = t.name ?? "";
                    //err.ParentName = this.name ?? "";
                    //err.ErrText = "Table failed validation";
                    //err.ErrorType = eErrorType.ValidationError;
                    //errs.Add(err);
                }

                // Roll up errors...
                foreach(var e in resval.errs)
                    errs.Add(e);
            }

            // Return fail if we accumulated errors...
            if(errs.Count == 0)
                return (1, errs);
            else
                return (-1, errs);
        }
    }
}
