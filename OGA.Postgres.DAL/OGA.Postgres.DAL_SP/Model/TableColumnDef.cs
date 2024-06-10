using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL.Model
{
    public class TableColumnDef
    {
        public string ColName { get; set; }

        public string ColType { get; set; }

        public string Collate { get; set; }

        public bool IsPk { get; set; }

        public bool CanBeNull { get; set; }

        public TableColumnDef()
        {

        }


        public override string ToString()
        {
            string sql = $"\"{this.ColName}\" {this.ColType}";

            // Add any collation...
            if(!string.IsNullOrEmpty(this.Collate))
                sql = sql + " " + this.Collate;

            // Add any not null constraint...
            if (!CanBeNull)
                sql = sql + " " + "NOT NULL";

            return sql;
        }
    }

    public enum ePkColTypes
    {
        /// <summary>
        /// UUID primary key
        /// </summary>
        uuid = 1,
        /// <summary>
        /// 32-bit integer primary key
        /// </summary>
        integer = 2,
        /// <summary>
        /// 64-bit integer primary key
        /// </summary>
        bigint = 3,
    }

    public enum eNumericColTypes
    {
        /// <summary>
        /// Covers .NET Int32 type
        /// </summary>
        integer = 1,
        /// <summary>
        /// Covers .NET Int64 type
        /// </summary>
        bigint = 2,
        /// <summary>
        /// Covers .NET float type
        /// </summary>
        real = 3,
        /// <summary>
        /// Covers .NET double type
        /// </summary>
        double_precision = 4,
        /// <summary>
        /// Covers .NET decimal type
        /// </summary>
        numeric = 5,
    }
}
