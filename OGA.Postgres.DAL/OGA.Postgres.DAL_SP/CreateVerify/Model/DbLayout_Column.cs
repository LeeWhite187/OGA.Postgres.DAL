using Newtonsoft.Json.Converters;
using OGA.Postgres.DAL_SP.CreateVerify.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace OGA.Postgres.DAL_SP.Model
{
    /// <summary>
    /// Holds the info necessary to create/verify a column of a database table.
    /// </summary>
    public class DbLayout_Column
    {
        /// <summary>
        /// Holds the column name.
        /// </summary>
        public string name { get; set; }

        /// <summary>
        /// Specifies the datatype need for the column.
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public eColDataTypes dataType { get; set; }

        /// <summary>
        /// Order in which columns get created.
        /// </summary>
        public int ordinal { get; set; }

        /// <summary>
        /// Used by the varchar type, to specify the max length.
        /// </summary>
        public int? maxlength { get; set; }

        /// <summary>
        /// Set if the column is an identity column.
        /// NOTE: Postgres only allows these datatypes as identity: smallint, int, ot bigint
        /// </summary>
        public bool isIdentity { get; set; }

        /// <summary>
        /// When isIdentity is true, this column defines when the column is auto-generated.
        /// This property is NULL when isIdentity is false.
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public eIdentityBehavior identityBehavior { get; set; }

        /// <summary>
        /// Set if the column can be NULL.
        /// </summary>
        public bool isNullable { get; set; }

        /// <summary>
        /// Default Constructor
        /// </summary>
        public DbLayout_Column()
        {
            name = "";
            dataType = eColDataTypes.notset;
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

            // Verify the column has a valid name...
            if(!Postgres_Tools.ColumnNameIsValid(name))
            {
                // The column name is not valid.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(DbLayout_Column)}:-:{nameof(Validate)} - " +
                    $"Invalid column name ({(this.name ?? "")}).");

                var err = new VerificationDelta();
                err.ObjType = eObjType.Column;
                err.ObjName = name ?? "";
                err.ParentName = parentname;
                err.ErrText = "Invalid column name";
                err.ErrorType = eErrorType.ValidationError;
                errs.Add(err);
            }
            if(this.ordinal <= 0)
            {
                // The column ordinal is invalid.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(DbLayout_Column)}:-:{nameof(Validate)} - " +
                    $"Invalid ordinal for column ({(this.name ?? "")}).");

                var err = new VerificationDelta();
                err.ObjType = eObjType.Column;
                err.ObjName = name ?? "";
                err.ParentName = parentname;
                err.ErrText = "Invalid column ordinal";
                err.ErrorType = eErrorType.ValidationError;
                errs.Add(err);
            }

            // Verify the column type is defined...
            if(this.dataType == eColDataTypes.notset)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(DbLayout_Column)}:-:{nameof(Validate)} - " +
                    $"Invalid datatype for column ({(this.name ?? "")}).");

                var err = new VerificationDelta();
                err.ObjType = eObjType.Column;
                err.ObjName = name ?? "";
                err.ParentName = parentname;
                err.ErrText = "Invalid column datatype";
                err.ErrorType = eErrorType.ValidationError;
                errs.Add(err);
            }

            // Verify a primary key column is not set as nullable...
            if(this.dataType == eColDataTypes.pk_integer ||
                this.dataType == eColDataTypes.pk_bigint ||
                this.dataType == eColDataTypes.pk_uuid ||
                this.dataType == eColDataTypes.pk_varchar)
            {
                // Column is a primary key.
                if(this.isNullable)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Column)}:-:{nameof(Validate)} - " +
                        $"Column ({(this.name ?? "")}) is set as primary key and nullable.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Column;
                    err.ObjName = name ?? "";
                    err.ParentName = parentname;
                    err.ErrText = "Column cannot be nullable primary key";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }
            }

            // Verify a varchar column has a length...
            if(this.dataType == eColDataTypes.varchar ||
                this.dataType == eColDataTypes.pk_varchar)
            {
                // Column is a varchar.
                if(this.maxlength == null || this.maxlength < 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Column)}:-:{nameof(Validate)} - " +
                        $"Varchar column ({(this.name ?? "")}) has invalid max length.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Column;
                    err.ObjName = name ?? "";
                    err.ParentName = parentname;
                    err.ErrText = "Invalid Max Length for Varchar Column";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }
            }

            // Verify an identity column is the allowed types and a behavior is defined...
            if(this.isIdentity)
            {
                // Postgres allows these to be identity: smallint, int, bigint.
                // We won't allow a smallint as identities, so, we will disregard them.
                if(this.dataType == eColDataTypes.bigint || this.dataType == eColDataTypes.pk_bigint ||
                    this.dataType == eColDataTypes.integer || this.dataType == eColDataTypes.pk_integer)
                {
                    // The column is an allowed identity type.
                }
                else
                {
                    // The column is NOT an allowed identity type.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Column)}:-:{nameof(Validate)} - " +
                        $"Column ({(this.name ?? "")}) is invalid type for identity.");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Column;
                    err.ObjName = name ?? "";
                    err.ParentName = parentname;
                    err.ErrText = "Invalid Datatype for Identity";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }

                // Check that a behavior is defined...
                if(this.identityBehavior == eIdentityBehavior.GenerateByDefault ||
                    this.identityBehavior == eIdentityBehavior.GenerateAlways)
                {
                    // These are the allowed behaviors.
                }
                else
                {
                    // Undefined behavior state when identity is set.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(DbLayout_Column)}:-:{nameof(Validate)} - " +
                        $"Column ({(this.name ?? "")}) is identity, but has invalid behavior type ({(this.identityBehavior.ToString())}).");

                    var err = new VerificationDelta();
                    err.ObjType = eObjType.Column;
                    err.ObjName = name ?? "";
                    err.ParentName = parentname;
                    err.ErrText = "Invalid Identity Behavior";
                    err.ErrorType = eErrorType.ValidationError;
                    errs.Add(err);
                }
            }

            // Return fail if we accumulated errors...
            if(errs.Count == 0)
                return (1, errs);
            else
                return (-1, errs);
        }

        /// <summary>
        /// Compares two column layouts, and returns 1 if same.
        /// Accepts an options instance to drive comparison behavior.
        /// </summary>
        /// <param name="c1"></param>
        /// <param name="c2"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        static public int CompareColumnLayouts(DbLayout_Column c1, DbLayout_Column c2, LayoutComparisonOptions options = null)
        {
            if (options == null)
                options = new LayoutComparisonOptions();

            if (c1 == null || c2 == null) return -1;

            if (c1.name != c2.name) return -1;

            if(c1.dataType != c2.dataType) return -1;

            // Check max length if varchar...
            if(c1.dataType == eColDataTypes.varchar ||
                c1.dataType == eColDataTypes.pk_varchar)
            {
                if(c1.maxlength != c2.maxlength) return -1;
            }

            if(c1.isIdentity != c2.isIdentity) return -1;

            if(c1.identityBehavior != c2.identityBehavior) return -1;

            if(c1.isNullable != c2.isNullable) return -1;

            // Check if we are to enforce ordinal matches...
            if(options.columnOrdinalsMustMatch)
            {
                if(c1.ordinal != c2.ordinal) return -1;
            }

            return 1;
        }
    }
}
