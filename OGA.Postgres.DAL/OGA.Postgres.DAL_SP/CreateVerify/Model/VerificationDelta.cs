using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL_SP.CreateVerify.Model
{
    /// <summary>
    /// Represents a single difference between a layout and a live database object.
    /// </summary>
    public class VerificationDelta
    {
        public eObjType ObjType { get; set; }
        public string ObjName { get; set; }
        public string ParentName { get; set; }

        public eErrorType ErrorType { get; set; }

        public string ErrText { get; set; }

    }

    public enum eErrorType
    {
        /// <summary>
        /// Failed to query/update database server.
        /// </summary>
        DatabaseAccessError,
        ValidationError,
        /// <summary>
        /// Object Not Found in Live Database object.
        /// </summary>
        NotFound,
        /// <summary>
        /// Object different in Live Database.
        /// </summary>
        Different,
        /// <summary>
        /// Object is extra in Live Database.
        /// </summary>
        Extra
    }

    public enum eObjType
    {
        Server,
        Layout,
        Column,
        Table,
        Database
    }
}
