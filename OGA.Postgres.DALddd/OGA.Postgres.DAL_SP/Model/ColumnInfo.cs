using System;
using System.Collections.Generic;
using System.Text;
using OGA.Postgres.DAL.CreateVerify.Model;

namespace OGA.Postgres.DAL.Model
{
    /// <summary>
    /// Holds the name and type of a table column.
    /// Used in schema queries for the structure of a database table.
    /// </summary>
    public class ColumnInfo
    {
        public string name { get; set; }

        public string dataType { get; set; }

        public int ordinal { get; set; }

        public int? maxlength { get; set; }

        public bool isPk { get; set; } = false;

        /// <summary>
        /// When isPk is true, this column defines when the column is auto-generated.
        /// This property is UNSET when isPk is false.
        /// </summary>
        public eIdentityBehavior identityBehavior { get; set; } = eIdentityBehavior.UNSET;


        public bool isNullable { get; set; }
    }
}
