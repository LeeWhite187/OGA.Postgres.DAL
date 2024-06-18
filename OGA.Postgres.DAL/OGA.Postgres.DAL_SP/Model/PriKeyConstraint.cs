using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL.Model
{
    /// <summary>
    /// Holds data for a single primary key constraint, of a table.
    /// </summary>
    public class PriKeyConstraint
    {
        public string table_schema { get; set; }

        public string table_name { get; set; }

        public string constraint_name { get; set; }

        public int position { get; set; }

        public string key_column { get; set; }


        public PriKeyConstraint()
        {
            table_schema = "";
            table_name = "";
            constraint_name = "";
            key_column = "";
        }
    }
}
