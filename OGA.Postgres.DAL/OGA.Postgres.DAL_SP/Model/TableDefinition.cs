using OGA.Postgres.DAL_SP.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Text;

namespace OGA.Postgres.DAL_SP
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

            if (string.IsNullOrEmpty(tablename))
                throw new Exception("Invalid table name");
            if (string.IsNullOrEmpty(owner))
                throw new Exception("Invalid owner");

            this.tablename = tablename;
            this.owner = owner;
        }


        /// <summary>
        /// Call this method to add a primary key column to the table.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="datatype"></param>
        /// <returns></returns>
        public int Add_Pk_Column(string colname, ePkColTypes datatype)
        {
            if(string.IsNullOrEmpty(colname))
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

            // Ensure a pk doesn't already exist...
            if(this.columnlist.Exists(m=>m.IsPk == true))
            {
                // A primary key column already exists.
                // Cannot add another one.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = true;
            cd.Collate = "";
            cd.CanBeNull = false;

            if (datatype == ePkColTypes.uuid)
                cd.ColType = "uuid";
            else if (datatype == ePkColTypes.integer)
                cd.ColType = "integer";
            else if (datatype == ePkColTypes.bigint)
                cd.ColType = "bigint";

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Adds a datetime column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_UTCDateTime_Column(string colname, bool canbenull)
        {
            if(string.IsNullOrEmpty(colname))
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
            cd.ColType = "timestamp without time zone";

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
            if(string.IsNullOrEmpty(colname))
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
            cd.ColType = "uuid";

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
            if(string.IsNullOrEmpty(colname))
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
                cd.ColType = "integer";
            else if (datatype == eNumericColTypes.bigint)
                cd.ColType = "bigint";
            else if (datatype == eNumericColTypes.real)
                cd.ColType = "real";
            else if (datatype == eNumericColTypes.double_precision)
                cd.ColType = "double precision";
            else if (datatype == eNumericColTypes.numeric)
                cd.ColType = "numeric";

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
            if(string.IsNullOrEmpty(colname))
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
                cd.ColType = "text";
                cd.Collate = "COLLATE pg_catalog.\"default\"";
            }
            else
            {
                cd.ColType = $"character varying({length.ToString()})";
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
