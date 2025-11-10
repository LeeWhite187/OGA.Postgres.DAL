using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL.CreateVerify.Model
{
    /// <summary>
    /// Options used to drive database and layout comparison behavior.
    /// Create an instance of this, if you need non-default reconciliation behavior for comparing databases and layouts.
    /// </summary>
    public class LayoutComparisonOptions
    {
        /// <summary>
        /// Used by DatabaseLayout_Tool:Verify_Database_Layout().
        /// When set, the presence of extra tables in the live database, are ignored (not listed as verification deltas).
        /// If clear, extra tables in the live database, are listed as verification deltas.
        /// Off by default (extra tables are listed as deltas).
        /// </summary>
        public bool ignoreExtraTablesInLiveDatabase { get; set; } = false;

        /// <summary>
        /// Used by DatabaseLayout_Tool:Verify_Database_Layout().
        /// When set, the presence of extra columns in live database tables, are ignored (not listed as verification deltas).
        /// If clear, extra columns in live database tables, are listed as verification deltas.
        /// Off by default (extra columns are listed as deltas).
        /// </summary>
        public bool ignoreExtraColumnsInLiveDatabaseTables { get; set; } = false;

        /// <summary>
        /// If set, the order of tables matters in a layout.
        /// If clear, table order is an ignored property, during comparisons.
        /// Off by default.
        /// </summary>
        public bool tableOrdinalsMustMatch { get; set; } = false;

        /// <summary>
        /// If set, the order of columns matters in a layout.
        /// If clear, column order is an ignored property, during comparisons.
        /// Off by default.
        /// </summary>
        public bool columnOrdinalsMustMatch { get; set; } = false;
    }
}
