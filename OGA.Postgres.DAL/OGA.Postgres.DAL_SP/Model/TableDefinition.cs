using OGA.Postgres.DAL.Model;
using OGA.Postgres.DAL.CreateVerify.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Text;
using OGA.Postgres.DAL_SP.Model;

namespace OGA.Postgres.DAL
{
    public class TableDefinition
    {
        private List<TableColumnDef> columnlist;

        public string tablename { get; private set; }

        public string owner { get; private set; }

        public string tablespace { get; set; } = "pg_default";


        public TableDefinition(string tablename, string owner)
        {
            columnlist = new List<TableColumnDef>();

            if (string.IsNullOrWhiteSpace(tablename))
                throw new Exception("Invalid table name");
            if (string.IsNullOrWhiteSpace(owner))
                throw new Exception("Invalid owner");

            this.tablename = tablename;
            this.owner = owner;
        }


        /// <summary>
        /// Adds a boolean column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_Boolean_Column(string colname, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.columnlist.Exists(m=>m.ColName == colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = false;
            cd.Collate = "";
            cd.CanBeNull = canbenull;
            cd.ColType = SQL_Datatype_Names.CONST_SQL_boolean;

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Call this method to add a primary key column to the table.
        /// Accepts an optional varchar length parameter, for primary keys of varchar type.
        /// Accepts an optional identity behavior parameter (identitybehavior), that is set if the column will generate its own sequence identifiers.
        /// NOTE: identitybehavior is ONLY for datatypes of bigint and integer. All other datatypes will fail validation.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="datatype"></param>
        /// <param name="identitybehavior"></param>
        /// <param name="varcharlength"></param>
        /// <returns></returns>
        public int Add_Pk_Column(string colname, ePkColTypes datatype, eIdentityBehavior identitybehavior = eIdentityBehavior.UNSET, int? varcharlength = null)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(TableDefinition)}:-:{nameof(Add_Pk_Column)} - " +
                    $"Column name is empty.");

                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.columnlist.Exists(m=>m.ColName == colname))
            {
                // Column name already exists.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(TableDefinition)}:-:{nameof(Add_Pk_Column)} - " +
                    $"Column name already exists.");

                return -1;
            }

            // Ensure a pk doesn't already exist...
            if(this.columnlist.Exists(m=>m.IsPk == true))
            {
                // A primary key column already exists.
                // Cannot add another one.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(TableDefinition)}:-:{nameof(Add_Pk_Column)} - " +
                    $"A Primary Key column already exists.");

                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = true;
            cd.Collate = "";
            cd.CanBeNull = false;

            // Set the datatype...
            if (datatype == ePkColTypes.uuid)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_uuid;
            else if (datatype == ePkColTypes.integer)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_integer;
            else if (datatype == ePkColTypes.bigint)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_bigint;
            else if (datatype == ePkColTypes.varchar)
            {
                // The caller wants to add a varchar primary key.
                // We will require it to have a defined max length.
                if(varcharlength == null)
                {
                    // Caller failed to give us a length for the varchar.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(TableDefinition)}:-:{nameof(Add_Pk_Column)} - " +
                        $"Cannot create varchar primary key without max length.");

                    return -2;
                }

                cd.ColType = SQL_Datatype_Names.CONST_SQL_character_varying + $"({varcharlength.Value.ToString()})";
            }

            // Set identity behavior clause...
            // We ignore it for invalid types.
            if(datatype == ePkColTypes.bigint ||
                datatype == ePkColTypes.integer)
            {
                // Datatype is valid for identity behavior usage.

                if (identitybehavior == eIdentityBehavior.GenerateByDefault)
                    cd.IdentityBehavior = eIdentityBehavior.GenerateByDefault;
                if(identitybehavior == eIdentityBehavior.GenerateAlways)
                    cd.IdentityBehavior = eIdentityBehavior.GenerateAlways;
            }

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Adds a non-UTC datetime column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_DateTime_Column(string colname, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.columnlist.Exists(m=>m.ColName == colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = false;
            cd.Collate = "";
            cd.CanBeNull = canbenull;
            // In PostgreSQL, "timestamp without time zone" is used to represent a non-UTC datetime.
            // See this: https://wiki.galaxydump.com/link/221
            cd.ColType = SQL_Datatype_Names.CONST_SQL_timestamp_without_time_zone;

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Adds a UTC datetime column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_UTCDateTime_Column(string colname, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.columnlist.Exists(m=>m.ColName == colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = false;
            cd.Collate = "";
            cd.CanBeNull = canbenull;
            // In PostgreSQL, "timestamp with time zone" is used to represent a UTC datetime.
            // See this: https://wiki.galaxydump.com/link/221
            cd.ColType = SQL_Datatype_Names.CONST_SQL_timestamp_with_time_zone;

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Adds a Guid (UUID) column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_Guid_Column(string colname, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.columnlist.Exists(m=>m.ColName == colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = false;
            cd.Collate = "";
            cd.CanBeNull = canbenull;
            cd.ColType = SQL_Datatype_Names.CONST_SQL_uuid;

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Adds a numeric column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="datatype"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_Numeric_Column(string colname, eNumericColTypes datatype, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.columnlist.Exists(m=>m.ColName == colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = false;
            cd.Collate = "";
            cd.CanBeNull = canbenull;

            if (datatype == eNumericColTypes.integer)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_integer;
            else if (datatype == eNumericColTypes.bigint)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_bigint;
            else if (datatype == eNumericColTypes.real)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_real;
            else if (datatype == eNumericColTypes.double_precision)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_double_precision;
            else if (datatype == eNumericColTypes.numeric)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_numeric;

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Adds a string column to the table schema.
        /// If length is zero, datatype is 'text'.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="length"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_String_Column(string colname, int length, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.columnlist.Exists(m=>m.ColName == colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = false;
            cd.Collate = "";
            cd.CanBeNull = canbenull;

            if(length <= 0)
            {
                cd.ColType = SQL_Datatype_Names.CONST_SQL_text;
                cd.Collate = "COLLATE pg_catalog.\"default\"";
            }
            else
            {
                cd.ColType = SQL_Datatype_Names.CONST_SQL_character_varying + $"({length.ToString()})";
                cd.Collate = "COLLATE pg_catalog.\"default\"";
            }

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// This method will compose the CREATE command to add the database table.
        /// </summary>
        /// <returns></returns>
        public string CreateSQLCmd()
        {
            StringBuilder b = new StringBuilder();

            // Create the table...
            b.AppendLine($"CREATE TABLE IF NOT EXISTS public.\"{this.tablename}\"");
            b.AppendLine($"(");

            // Add its columns...
            foreach(var c in this.columnlist)
                b.AppendLine("\t" + c.ToString() + ",");

            // Add a primary key constraint if needed...
            try
            {
                var pkc = this.columnlist.Find(n => n.IsPk == true);
                if(pkc != null)
                {
                    // Build a constraint string for it...
                    b.AppendLine("\t" + $"CONSTRAINT \"{this.tablename}_pkey\" PRIMARY KEY (\"{pkc.ColName}\")");
                }
            } catch(Exception) { }

            b.AppendLine($")");
            // Set its table space...
            b.AppendLine($"TABLESPACE {this.tablespace};");

            // Set the table owner...
            b.AppendLine($"ALTER TABLE IF EXISTS public.\"{this.tablename}\" OWNER TO {this.owner};");

            // Give all privileges to the owner...
            b.AppendLine($"GRANT ALL ON TABLE public.\"{this.tablename}\" TO {this.owner};");

            return b.ToString();
        }
    }
}
