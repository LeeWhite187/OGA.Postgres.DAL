using OGA.Postgres.DAL_SP.CreateVerify.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL_SP.Model
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

        public bool isIdentity { get; set; } = false;

        /// <summary>
        /// When isIdentity is true, this column defines when the column is auto-generated.
        /// This property is UNSET when isIdentity is false.
        /// </summary>
        public eIdentityBehavior identityBehavior { get; set; } = eIdentityBehavior.UNSET;


        public bool isNullable { get; set; }
    }
}
