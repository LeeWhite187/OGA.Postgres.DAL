using OGA.Postgres.DAL.CreateVerify.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OGA.Postgres.DAL.Model
{
    /// <summary>
    /// Holds the info necessary to create/verify a database table.
    /// </summary>
    public class DbLayout_Table
    {
        /// <summary>
        /// Holds the table name.
        /// </summary>
        public string name { get; set; }

        /// <summary>
        /// Order in which tables get created.
        /// </summary>
        public int ordinal { get; set; }

        /// <summary>
        /// Holds the list of columns the table contains.
        /// </summary>
        public List<DbLayout_Column> columns { get; set; }


        /// <summary>
        /// Default Constructor
        /// </summary>
        public DbLayout_Table()
        {
            name = "";
            columns = new List<DbLayout_Column>();
        }


        /// <summary>
        /// Call this method to validate the table layout, before a create or verification is performed.
        /// Returns 1 if passed validation.
        /// Negatives and errors for failures.
        /// </summary>
        /// <returns></returns>
        public (int res, List<VerificationDelta> errs) Validate(string parentname)
        {
            var errs = new List<VerificationDelta>();

            // Verify the table has a valid name...
            if(!Postgres_Tools.TableNameIsValid(name))
            {
                // The table name is not valid.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(DbLayout_Table)}:-:{nameof(Validate)} - " +
                    $"Invalid table name ({(this.name ?? "")}).");

                var err = new VerificationDelta();
                err.ObjType = eObjType.Table;
                err.ObjName = name ?? "";
                err.ParentName = parentname;
                err.ErrText = "Invalid table name";
                err.ErrorType = eErrorType.ValidationError;
                errs.Add(err);
            }
            if(this.ordinal <= 0)
            {
                // The table ordinal is invalid.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(DbLayout_Table)}:-:{nameof(Validate)} - " +
                    $"Invalid ordinal for table ({(this.name ?? "")}).");

                var err = new VerificationDelta();
                err.ObjType = eObjType.Table;
                err.ObjName = name ?? "";
                err.ParentName = parentname;
                err.ErrText = "Invalid table ordinal";
                err.ErrorType = eErrorType.ValidationError;
                errs.Add(err);
            }

            // Verify columns...
            bool foundpk = false;
            foreach(var c in this.columns)
            {
                if(c == null)
                {
                    // Column is null.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Table)}:-:{nameof(Validate)} - " +
                        $"Null column encountered.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Table;
                    err.ObjName = name ?? "";
                    err.ParentName = parentname;
                    err.ErrText = "Null table";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                    continue;
                }
                /* Skipping column name validation, here, as it's done by the column's own validate method.
                if(!Postgres_Tools.ColumnNameIsValid(c.name))
                {
                    // Column name is not valid.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Table)}:-:{nameof(Validate)} - " +
                        $"Invalid column name ({(c.name ?? "")}) of table ({(this.name)}).");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Column;
                    err.ObjName = c.name ?? "";
                    err.ParentName = this.name;
                    err.ErrText = "Invalid column name";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }
                */
                // Verify column ordinal is unique...
                if(this.columns.Exists(n => n.ordinal == c.ordinal && n.name != c.name))
                {
                    // Column has duplicate ordinal.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Table)}:-:{nameof(Validate)} - " +
                        $"Duplicate ordinal for column ({(c.name ?? "")}) of table ({(this.name)}).");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Column;
                    err.ObjName = c.name ?? "";
                    err.ParentName = this.name ?? "";
                    err.ErrText = "Duplicate column ordinal";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }

                if(c.dataType == CreateVerify.Model.eColDataTypes.pk_bigint ||
                    c.dataType == CreateVerify.Model.eColDataTypes.pk_integer ||
                    c.dataType == CreateVerify.Model.eColDataTypes.pk_uuid ||
                    c.dataType == CreateVerify.Model.eColDataTypes.pk_varchar)
                {
                    // Current column is the primary key.
                    // Ensure this is the only key we've found...
                    if(foundpk)
                    {
                        // We've already identified a primary key for this table.
                        // We will flag a validation error for this second primary key.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{nameof(DbLayout_Table)}:-:{nameof(Validate)} - " +
                            $"Table ({(this.name)}) layout has multiple primary key columns.");

                        var err = new VerificationDelta();
                        err.ObjType = eObjType.Table;
                        err.ObjName = this.name ?? "";
                        err.ParentName = parentname;
                        err.ErrText = "Multiple Primary Key Columns";
                        err.ErrorType = eErrorType.ValidationError;
                        errs.Add(err);
                    }

                    foundpk = true;
                }
            }

            // Verify each column...
            foreach(var c in this.columns)
            {
                if(c == null)
                {
                    // Column is null.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Table)}:-:{nameof(Validate)} - " +
                        $"Null column encountered.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Table;
                    err.ObjName = name ?? "";
                    err.ParentName = parentname;
                    err.ErrText = "Null table";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                    continue;
                }

                // Validate the column's data...
                var resval = c.Validate(this.name ?? "");
                if(resval.res != 1)
                {
                    // Layout failed validation.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Table)}:-:{nameof(Validate)} - " +
                        $"Column ({(c.name ?? "")}) of table ({(this.name ?? "")}) failed validation.");

                    // Column validation method already created a specific error.
                    //var err = new VerificationDelta();
                    //err.ObjType = eObjType.Column;
                    //err.ObjName = c.name ?? "";
                    //err.ParentName = name ?? "";
                    //err.ErrText = "Column failed validation";
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

        /// <summary>
        /// Compares two table layouts, and returns 1 if same.
        /// Accepts an options instance to drive comparison behavior.
        /// </summary>
        /// <param name="t1"></param>
        /// <param name="t2"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        static public int CompareTableLayouts(DbLayout_Table t1, DbLayout_Table t2, LayoutComparisonOptions options = null)
        {
            if (options == null)
                options = new LayoutComparisonOptions();

            if (t1 == null || t2 == null)
                return -1;

            if (t1.name != t2.name)
                return -1;

            // Check if ordinals must match...
            if(options.tableOrdinalsMustMatch)
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
                var rescolcmp = DbLayout_Column.CompareColumnLayouts(c, c2, options);
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
    }
}
