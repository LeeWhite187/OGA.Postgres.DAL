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

        public bool isIdentity { get; set; }

        public bool isNullable { get; set; }
    }
}
